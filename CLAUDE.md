# ES Usage Gateway — Project Guide

## What This Is

A reverse proxy gateway that sits in front of Elasticsearch, intercepts all traffic, extracts usage metadata from requests (which fields are queried, filtered, aggregated, sorted, fetched), and computes field-level heat scores. Includes a control panel UI and Kibana dashboards.

## Architecture

- **ES + Kibana**: Docker containers via `docker-compose.yml` (ports 9200, 5601)
- **Gateway**: Local Python process — FastAPI + httpx async proxy (port 9301)
- **UI**: Inline HTML served by gateway at `/_gateway/ui` (no build step)

## Running

```bash
docker compose up -d                    # Start ES + Kibana
python -m generator.seed                # Seed products index (once)
python -m gateway.main                  # Start gateway on :9301
python kibana_setup.py --no-wait        # Import Kibana dashboards
```

## Project Structure

```
gateway/main.py       — FastAPI app, all routes
gateway/proxy.py      — Reverse proxy (httpx)
gateway/extractor.py  — DSL field extraction from ES requests
gateway/events.py     — Usage event model + emission to .usage-events index
gateway/analyzer.py   — Heat score computation
gateway/ui.py         — Control panel HTML (inline, no framework)
generator/seed.py     — Load sample products into ES
generator/generate.py — CLI traffic generator
generator/queries.py  — Query templates with weighted distribution
kibana_setup.py       — Programmatic Kibana dashboard/visualization setup
config.py             — ES host, gateway port, index names
```

## Code Conventions

- Python 3.10+, type hints where helpful
- FastAPI for HTTP, httpx for async HTTP client
- No ORMs — direct ES REST API via httpx
- Gateway UI is vanilla HTML/CSS/JS in a Python string (`gateway/ui.py`)
  - **Important**: JS template literals (`${...}`) inside the Python triple-quoted string — use `\\n` not `\n` for JS newlines, otherwise Python interprets the escape
- Logging via stdlib `logging`, not print()

## Git Workflow

### Branch Strategy
- `master` — stable, working code
- Feature branches: `feat/<short-name>` (e.g., `feat/add-sql-parser`)
- Bug fixes: `fix/<short-name>` (e.g., `fix/slider-rendering`)
- No long-lived branches — merge and delete

### When to Commit
- **After completing a logical unit of work** — one feature, one fix, one refactor
- **Before starting something risky** — commit working state first
- **Not in the middle** — don't commit half-done features or broken code
- Typical granularity: 1-3 commits per feature, each independently meaningful

### Commit Messages
Format:
```
<type>: <what changed> (concise, imperative mood)

<optional body — why, not what>

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
```

Types: `feat`, `fix`, `refactor`, `docs`, `chore`

Examples:
- `feat: add per-query-type breakdown to generator UI`
- `fix: escape JS newline in Python string to prevent syntax error`
- `refactor: replace tag clouds with data tables in Kibana dashboard`
- `chore: add .gitignore and initialize git repo`

### What NOT to Commit
- `__pycache__/`, `.pyc` files
- `.claude/` directory
- `.env` or credential files
- Docker volumes / ES data
- IDE config (`.vscode/`, `.idea/`)
