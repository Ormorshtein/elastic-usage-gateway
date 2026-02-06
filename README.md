# ES Usage Gateway — MVP

A reverse-proxy gateway for Elasticsearch that observes query traffic and computes index-level and field-level heat scores. Helps identify hot/warm/cold/unused indices and fields for ILM and mapping optimization.

## Architecture

```
Query Generator → Gateway (port 9201) → Elasticsearch (port 9200)
                     │
                     └──▶ .usage-events index → Heat Analyzer
```

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
| `GATEWAY_PORT` | `9201` | Gateway listen port |
| `USAGE_INDEX` | `.usage-events` | Index for storing usage events |
| `CLUSTER_ID` | `default` | Cluster identifier |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /_gateway/health` | Gateway health check |
| `GET /_gateway/heat?hours=24` | Heat report for the last N hours |
| `* /{path}` | All other traffic proxied to Elasticsearch |
