# Applicative Load Observability — Elastic Usage Gateway

**Product specification & vision**

---

## 1. Product Overview

This project aims to provide **applicative load observability** for Elasticsearch deployments running on **ECK (Elastic Cloud on Kubernetes)**. Given a client’s ECK cluster, the system should:

1. **Identify the most loading queries** — which requests consume the most resources and contribute most to contention.
2. **Enable panel creation** — surface this data in dashboards so operators and developers can analyze and act on it.

The end goal is to **understand what is causing resource contention** (storage, CPU, memory, network) and **correlate it with queries and users** so the source of load can be clearly attributed.

---

## 2. Core Concepts

### 2.1 Stress Score

A **stress score** is a synthetic metric that quantifies how “heavy” or costly a query is. Example inputs:

- **Execution time** — e.g. time from request start to response.
- (Future) Other factors such as resource usage, result size, or shard involvement.

The stress score allows:

- Ranking queries by impact.
- Comparing similar queries (e.g. same template, different parameters).
- Correlating high-stress queries with resource contention.

---

### 2.2 Query Analysis

For each query (or query template), the system should capture:

| Dimension | Description |
|-----------|-------------|
| **Operation type** | Whether the query is **geo**, **text** (full-text search), **aggregation**, **kNN**, etc. This helps understand which workloads dominate. |
| **Query scrubbing / templating** | Normalize the query (e.g. remove literal values, IDs, dates) to get a **query template**. This enables: <br>• Detecting **repeated** or **high-frequency** patterns. <br>• Grouping identical logical queries for aggregation and stress scoring. |

Outcomes:

- “Top N most loading query templates.”
- “Which operation types contribute most to CPU/memory/network.”
- “Which templates run most often and at what stress.”

---

### 2.3 User / Requester Analysis

To attribute load to **who** and **what** is sending it:

| Dimension | Description |
|-----------|-------------|
| **Hostname** | Machine or pod from which the request originated. |
| **Username** | Elasticsearch user (or API key identity) performing the query. |
| **Applicative provider** | Name of the application or service that uses Elasticsearch (e.g. “search-api”, “recommendation-service”). |

Outcomes:

- “Which host/application/user is driving the most load.”
- Correlation with stress score and query templates for root-cause analysis.

---

## 3. Resource Contention & Correlation

Resources in scope:

- **Storage** — disk I/O, index size growth, segment merging.
- **CPU** — search and indexing CPU time.
- **Memory** — heap, caches, circuit breakers.
- **Network** — bandwidth and latency between nodes and clients.

The system should support:

1. **Observing contention** — when and where each resource is under pressure (e.g. high CPU, memory pressure, slow disk).
2. **Correlating with queries** — which query templates and operation types spike when contention appears.
3. **Correlating with users** — which hostnames, usernames, and applicative providers are active during those spikes.

**Target outcome:** Answer questions like:  
*“The CPU spike at 14:00 was driven by aggregation queries from application X, host Y, and the top contributing template was Z.”*

---

## 4. Recommended Dashboards for Best Analysis

To get the most value from the data above, the following dashboards are recommended.

### Dashboard 1: **Top Loading Queries (by stress & template)**

- **Purpose:** See which logical queries (templates) contribute most to load.
- **Panels:**
  - Table: query template, operation type, total stress score, execution count, avg execution time.
  - Bar chart: top N query templates by stress score (e.g. last 24h).
  - Time series: stress score over time, optionally broken down by template or operation type.
- **Use case:** Prioritize optimization (slow or high-stress templates first).

---

### Dashboard 2: **Query Template Frequency & Repetition**

- **Purpose:** Understand how often the same logical query runs (e.g. N+1 patterns, repeated heavy queries).
- **Panels:**
  - Table: template, execution count, unique callers (e.g. hostname or app), time range.
  - Time series: execution count per template over time.
  - Heatmap or distribution: template vs. execution count to spot “noisy” templates.
- **Use case:** Find candidates for caching, batching, or deduplication.

---

### Dashboard 3: **Operation Type Breakdown**

- **Purpose:** See which workload types (geo, text, aggregation, etc.) dominate.
- **Panels:**
  - Pie or bar: share of requests/stress by operation type.
  - Time series: request count or stress score by operation type over time.
  - Table: operation type, count, total stress, avg time.
- **Use case:** Balance capacity and tuning (e.g. more CPU for aggregations vs. memory for text search).

---

### Dashboard 4: **Load by User / Application / Host**

- **Purpose:** Attribute load to hostname, username, and applicative provider.
- **Panels:**
  - Table: hostname (or app name), username, request count, total stress score, avg time.
  - Bar chart: top hosts or applications by stress score.
  - Time series: stress or request rate over time, split by application or host.
- **Use case:** “Which app or host is causing the spike?” and capacity/ownership discussions.

---

### Dashboard 5: **Resource Contention vs. Queries (Correlation)**

- **Purpose:** Link resource metrics (CPU, memory, network, storage) to query and user activity.
- **Panels:**
  - Time series: resource metrics (e.g. CPU %, heap usage, disk IO, network bytes) on one axis.
  - Overlay or aligned time series: stress score or request rate by template or by application.
  - Table or list: “Top templates / apps during contention window” (e.g. select a time range of high CPU and see top queries in that window).
- **Use case:** Root cause — “This CPU spike was caused by these query templates from this application.”

---

### Dashboard 6: **Stress Score Distribution & Trends**

- **Purpose:** Monitor overall “heaviness” of the cluster and spot regressions.
- **Panels:**
  - Time series: p50, p95, p99 stress score over time.
  - Histogram: distribution of stress scores (e.g. per hour).
  - Single stat or gauge: current vs. previous period (e.g. avg stress score).
- **Use case:** SLOs and trend analysis (e.g. “queries got heavier after deployment X”).

---

## 5. Summary

| Goal | How it’s achieved |
|------|-------------------|
| Find most loading queries | Stress score + query template aggregation; Dashboard 1. |
| Quantify “heaviness” | Stress score (e.g. time-based, later resource-aware). |
| Query analysis | Operation type + scrubbing/templating; Dashboards 1, 2, 3. |
| User/requester analysis | Hostname, username, applicative provider; Dashboard 4. |
| Resource contention & source | Correlation of resources with templates and users; Dashboard 5. |
| Panels for analysis | Dashboards 1–6 above. |

This README describes the **intended product and analysis experience**. Implementation details and code may evolve; this document serves as the specification for what “done” looks like from an observability and dashboard perspective.
