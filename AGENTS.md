# MoneyBin

Personal financial data platform. Python + DuckDB + SQLMesh + Typer CLI + MCP server.

## Design Philosophy

- **Sync server is opaque.** The client communicates only with moneybin-server's API surface. External service providers are implementation details hidden behind the server.

## Critical Rules

- **Package manager**: `uv` only. Never `pip install`, `uv pip install`, or `python -m`.
- **Linting/formatting**: `make format && make lint` (Ruff, line length 88).
- **Type checking**: `uv run pyright` on modified files (not mypy).
- **Tests**: Dev: `uv run pytest tests/path/to/test_file.py -v`. Pre-commit: `make test`. Always `uv run pytest` — never `uv run python -m pytest` (sandbox-denied per the `python -m` ban above). If `uv run pytest` resolves to the wrong interpreter, the venv has stale shebangs from a worktree move; fix with `uv sync --reinstall` rather than working around it via `python -m`.
- **Pre-commit checklist**: `make check test` — format, lint, type-check, tests. Run once before committing.
- **SQL formatting**: `uv run sqlmesh -p sqlmesh format`.
- **Check library docs first**: Before implementing patterns with SQLMesh, DuckDB, Pydantic, etc., verify the correct API in official docs. Training knowledge may be outdated.

## Key Abstractions

| Need | Use | Never |
|------|-----|-------|
| Database access | `get_database()` → `Database` | `duckdb.connect()` |
| Configuration | `get_settings()` → `MoneyBinSettings` | `os.getenv()`, hardcoded values |
| Secrets/keys | `SecretStore` | `os.getenv()`, plain `str` fields |
| Table references | `TableRef.FCT_TRANSACTIONS`, etc. | Hardcoded table name strings |
| DataFrames | DuckDB > Polars > Pandas | Pandas (unless required for library compat — document why) |

## Code Standards

- **Logging**: `logger = logging.getLogger(__name__)`. Always f-strings — never `%s`/`%d` lazy formatting (bypasses `SanitizedLogFormatter`).
- **Inline SQL**: Triple-quoted strings (`"""..."""`).
- **Suppression comments**: Always include a reason: `# noqa: S608  # test input, not executing SQL`.
- **Acronyms**: ALL CAPS in class names: `OFXExtractor`, `CSVReader`, `PDFExtractor`.
- **Comments and docstrings**: Default to one short line. A longer comment or
  multi-paragraph module docstring is warranted when it documents a
  *non-obvious why* a future reader would otherwise undo — a workaround for an
  upstream bug, a hidden constraint, a platform-specific quirk, or an
  invariant the code relies on but doesn't enforce. Don't restate what the
  code already says; do explain context that lives outside the code.

## Architecture: Data Layers

| Layer | Schema | Materialized | Purpose |
|-------|--------|-------------|---------|
| Raw | `raw` | Table | Untouched data from loaders (Python) |
| Staging | `prep` | View | Light cleaning, type casting (SQLMesh `stg_*`) |
| Core | `core` | Table | Canonical, deduplicated, multi-source |
| User state | `app` | Table | User-authored state on canonical entities (categorizations, notes, tags, audit log, account settings, proposed rules). Written by services; presented to consumers via `core.*` joins. |
| Aggregations | `agg_*` / `reports` | View / Table | Cross-entity rollups (`agg_net_worth`) and analytical recipes (`reports.cash_flow`, `reports.uncategorized_queue`, etc.). Consumer-facing read surface for dashboards and MCP tools. |

1. **One canonical table per entity** — `dim_accounts`, `fct_transactions`, etc. Consumers read from core (and `app`/`reports` views) only.
2. **Multi-source union** — Core models `UNION ALL` from every staging source with `source_system` column.
3. **Dedup in core** — `ROW_NUMBER()` windows for duplicates; mapping tables for cross-source dedup.
4. **Accounting sign convention** — negative = expense, positive = income. `DECIMAL(18,2)` for amounts, `DATE` for dates.
5. **Source-agnostic consumers** — MCP server, CLI use `TableRef` constants, never source-specific logic.
6. **`app.*` is user state, not raw data** — anything authored by the user (categorizations, notes, tags, settings, audit history) lives here. Services are the only writers; presentation joins back through `core.*`.

## Specs & Implementation Tracking

Feature specs live in `docs/specs/`. The **[Spec Index](docs/specs/INDEX.md)** is the single source of truth.

- **Before implementing**, check `INDEX.md` for existing specs.
- **When starting**, update status to `in-progress` (spec file + `INDEX.md`).
- **When complete**, update to `implemented`. See `.claude/rules/shipping.md` for README updates.
- **Observability wiring**: Specs touching app code must include metrics. See `docs/specs/observability.md` and `src/moneybin/metrics/registry.py`.
- Statuses: `draft` → `ready` → `in-progress` → `implemented`.

## Plans vs Specs

Specs (intent, durable) live in `docs/specs/` and are tracked. Implementation plans (step-by-step scaffolding from `superpowers:writing-plans` and similar) are ephemeral and **do not belong in the repo** — write them to `private/plans/` (gitignored). From a worktree, that is `../../private/plans/`. Before discarding a plan, lift any durable design rationale into the relevant spec or an ADR.

## Configuration

All config in `src/moneybin/config.py` — one `MoneyBinSettings` root via Pydantic Settings. Never hardcode paths, credentials, or tunable parameters. Env vars use `MONEYBIN_` prefix with `__` for nesting: `MONEYBIN_DATABASE__PATH`.

## Constants

Security-critical parameters (crypto cost factors, key lengths, salt sizes) defined once — module-level `_CONSTANTS` or config fields. Never duplicate across call sites.

## Security

- **Encryption at rest**: AES-256-GCM on all DuckDB databases. See [`privacy-data-protection.md`](docs/specs/privacy-data-protection.md).
- **No PII or financial data in logs.** Log record counts, IDs, and status codes only.
- **Parameterized SQL** with `?` placeholders. See `.claude/rules/security.md` for full standards.

## Rules Index

Files in `.claude/rules/` auto-load via Claude Code's `paths:` frontmatter — path-scoped rules load when Claude reads a matching file; unscoped rules load every session. The table below is for discoverability when planning work that hasn't touched matching files yet. Read a rule directly if you need it before editing.

### Path-scoped

| Rule | Covers |
|------|--------|
| `security.md` | SQL injection, input validation, XSS, PII, exception wrapping |
| `database.md` | DuckDB patterns, SQL conventions, schema, column comments |
| `mcp-server.md` | Tool taxonomy, response envelope, sensitivity tiers, services |
| `cli.md` | Typer patterns, error handling, command registration, icons |
| `testing.md` | Pytest patterns, fixtures, mocking strategy, DB test helpers |
| `data-extraction.md` | Incremental sync, dedup, parameter design, new data sources |
| `identifiers.md` | Content hashes, truncated UUIDs, source IDs, semantic slugs |
| `documentation.md` | Diagram conventions (Mermaid over ASCII) |

### Always loaded (workflow rules)

| Rule | Covers |
|------|--------|
| `shipping.md` | Post-implementation checklist: README updates, roadmap icons, `/simplify` pre-push pass |
| `branching.md` | Branch prefix → PR label mapping, commit message style |
| `sandboxing.md` | Bash invocation patterns: single commands, allowlisted pipelines, structured-output filtering, policy denials |
