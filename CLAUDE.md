# ES Usage Gateway — Project Guide

## Project Goal

The goal is to measure field-level usage from real Elasticsearch traffic — which fields are queried, filtered, aggregated, sorted, and written — so teams can make data-driven decisions about mapping optimization, ILM tiering, and capacity planning. No tool in the ES ecosystem does this today.

**Every task should be evaluated through this lens:** Does it help us observe more accurately, analyze more usefully, or deliver clearer recommendations? If a feature doesn't serve field-level usage intelligence, it probably doesn't belong here.

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

## Keeping Docs in Sync

After completing work, update docs to stay in sync:

- **CHANGELOG.md** — Add an entry for any new feature, bug fix, or significant change
- **ARCHITECTURE.md** — Update if project structure, API endpoints, or components changed
- **This file** — Update if new coding standards or gotchas were discovered
