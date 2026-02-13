# ES Usage Gateway — Project Guide

## Project Goal

The goal is to measure field-level usage and query patterns from real Elasticsearch traffic — which fields are queried, filtered, aggregated, sorted, and written, and which query shapes consume the most cluster time — so teams can make data-driven decisions about mapping optimization, ILM tiering, capacity planning, and query performance tuning. No tool in the ES ecosystem does this today.

**Every task should be evaluated through this lens:** Does it help us observe more accurately, analyze more usefully, or deliver clearer recommendations? If a feature doesn't serve field-level usage intelligence or hot query identification, it probably doesn't belong here.

## Audience

This project ships to an on-prem environment and will be maintained by a team of junior engineers. Optimize for readability and simplicity:

- Prefer explicit over clever. Obvious code > elegant code.
- Prefer flat over nested. Avoid deep abstractions or indirection layers that require jumping between files to understand a flow.
- Name things for clarity, not brevity. A long descriptive name is better than a short ambiguous one.
- Keep dependencies minimal. Every added library is something the team needs to learn and maintain.
- Comments should explain *why*, not *what*. If the *what* isn't obvious, simplify the code.

## Code Conventions

- Python 3.10+, type hints where helpful
- FastAPI for HTTP, httpx for async HTTP client
- No ORMs — direct ES REST API via httpx
- Gateway UI is vanilla HTML/CSS/JS in `gateway/ui.html` (no build step)
  - Read from disk on each request — edit and refresh browser, no gateway restart needed
- Logging via stdlib `logging`, not print()

## Testing

```bash
pytest tests/
```

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

### What NOT to Commit
- `__pycache__/`, `.pyc` files
- `.claude/` directory
- `.env` or credential files
- Docker volumes / ES data
- IDE config (`.vscode/`, `.idea/`)

## Operational Gotchas

### Gateway restart required for Python changes

All Python code (`main.py`, `extractor.py`, `events.py`, `mapping_diff.py`, etc.) requires a **gateway process restart** to take effect. The only exception is `gateway/ui.html`, which is read from disk on each request.

If you add a new endpoint, change extraction logic, or modify any `.py` file — kill the running gateway and restart it before testing. Otherwise the old code is still running and you'll get confusing results (e.g., new routes returning ES errors because the catch-all proxy forwards them to Elasticsearch).

On Windows:
```powershell
# Find the PID
Get-NetTCPConnection -LocalPort 9301 | Select-Object OwningProcess -Unique
# Kill it
Stop-Process -Id <PID> -Force
# Restart
python -m gateway.main  # or run in background
```

### Run `kibana_setup.py` after changing dashboards

Dashboard definitions in `kibana_setup.py` are declarative — they only take effect when you run the script to import them into Kibana. Editing the file alone does nothing.

```bash
python kibana_setup.py --no-wait
```

Always run this after modifying any visualization, dashboard, or control definition. The script uses `overwrite=true` so it's safe to re-run.

### PowerShell `curl` alias

On Windows, PowerShell aliases `curl` to `Invoke-WebRequest`, which has different syntax. Always use `curl.exe` explicitly:
```powershell
curl.exe -X POST http://localhost:9301/_gateway/mapping-diff/refresh
```

## Planning Rules

### Trace every field to the user-facing layer

When adding a field to an ES index (e.g., `last_seen` in `.mapping-diff`), the plan **must** specify how it surfaces in Kibana — which dashboard, which panel, which column, which metric type. Data that exists in the index but isn't visible in a dashboard is invisible to users and effectively doesn't exist.

**Checklist for new fields:**
1. ES index mapping — field name, type
2. Code that writes the field — which function, what value
3. Kibana visualization — which panel shows it, what agg type (`max`, `avg`, `terms`, etc.)
4. Dashboard placement — where on the grid, panel title

### Walk the user workflow end-to-end before marking done

Before marking a deliverable complete, walk through the user's workflow from start to finish:
- "I open the dashboard → I see X → I click/filter → I see Y → I can act on Z"

If any step requires manual workaround (e.g., "go to Discover and type this KQL query"), it's not done — build the dashboard or control that eliminates the manual step.

### Include everything discussed before planning

When creating an implementation plan, review all discussion points from the conversation and ensure each one maps to a concrete step. Agreed-upon features that aren't in the plan won't get implemented.

## Kibana Dashboards

Dashboards are the primary way users consume this tool's output. They should be **self-explanatory** — a new team member opening the dashboard for the first time should understand what they're looking at without reading docs or asking someone.

- Use **Markdown visualization panels** as section headers to break the dashboard into logical groups. Each section header should include a short explanation of what the section shows and why it matters.
- Panel titles should be descriptive enough to stand alone (e.g., "Costliest Query Templates (by total cluster time)" not "Templates").
- When a panel's meaning isn't obvious from the title, add a description in the saved object's `description` field.
- Prefer tables and bar charts over abstract visualizations — the audience is engineers making decisions, not executives watching a wall screen.

## Keeping Docs in Sync

After completing work, update docs to stay in sync:

- **CHANGELOG.md** — Add an entry for any new feature, bug fix, or significant change
- **ARCHITECTURE.md** — Update if project structure, API endpoints, or components changed
- **This file** — Update if new coding standards or gotchas were discovered
