# MoneyBin

Personal financial data platform. Python + DuckDB + SQLMesh + Typer CLI + MCP server.

## Design Philosophy

- **Sync server is opaque.** The client communicates only with moneybin-server's API surface. External service providers are implementation details hidden behind the server.

## Critical Rules

- **Package manager**: `uv` only. Never `pip install`, `uv pip install`, or `python -m`.
- **Linting/formatting**: `make format && make lint` (Ruff, line length 88).
- **Type checking**: `uv run pyright` on modified files (not mypy).
- **Tests**: Dev: `uv run pytest tests/path/to/test_file.py -v`. Pre-commit: `make test`.
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

## Architecture: Data Layers

| Layer | Schema | Materialized | Purpose |
|-------|--------|-------------|---------|
| Raw | `raw` | Table | Untouched data from loaders (Python) |
| Staging | `prep` | View | Light cleaning, type casting (SQLMesh `stg_*`) |
| Core | `core` | Table | Canonical, deduplicated, multi-source |

1. **One canonical table per entity** — `dim_accounts`, `fct_transactions`, etc. Consumers read from core only.
2. **Multi-source union** — Core models `UNION ALL` from every staging source with `source_system` column.
3. **Dedup in core** — `ROW_NUMBER()` windows for duplicates; mapping tables for cross-source dedup.
4. **Accounting sign convention** — negative = expense, positive = income. `DECIMAL(18,2)` for amounts, `DATE` for dates.
5. **Source-agnostic consumers** — MCP server, CLI use `TableRef` constants, never source-specific logic.

## Specs & Implementation Tracking

Feature specs live in `docs/specs/`. The **[Spec Index](docs/specs/INDEX.md)** is the single source of truth.

- **Before implementing**, check `INDEX.md` for existing specs.
- **When starting**, update status to `in-progress` (spec file + `INDEX.md`).
- **When complete**, update to `implemented`. See `.claude/rules/shipping.md` for README updates.
- **Observability wiring**: Specs touching app code must include metrics. See `docs/specs/observability.md` and `src/moneybin/metrics/registry.py`.
- Statuses: `draft` → `ready` → `in-progress` → `implemented`.

## Configuration

All config in `src/moneybin/config.py` — one `MoneyBinSettings` root via Pydantic Settings. Never hardcode paths, credentials, or tunable parameters. Env vars use `MONEYBIN_` prefix with `__` for nesting: `MONEYBIN_DATABASE__PATH`.

## Constants

Security-critical parameters (crypto cost factors, key lengths, salt sizes) defined once — module-level `_CONSTANTS` or config fields. Never duplicate across call sites.

## Security

- **Encryption at rest**: AES-256-GCM on all DuckDB databases. See [`privacy-data-protection.md`](docs/specs/privacy-data-protection.md).
- **No PII or financial data in logs.** Log record counts, IDs, and status codes only.
- **Parameterized SQL** with `?` placeholders. See `.claude/rules/security.md` for full standards.

## Conditional Rules Index

These `.claude/rules/` files load only when editing matching files. If you need guidance outside the current glob match, read the relevant file directly.

| Rule | Covers | Loads for |
|------|--------|-----------|
| `security.md` | SQL injection, input validation, XSS, PII, exception wrapping | `src/moneybin/**/*.py`, `**/*.sql` |
| `database.md` | DuckDB patterns, SQL conventions, schema, column comments | `**/*.sql`, `sqlmesh/**`, `database.py`, `schema.py`, `loaders/**` |
| `mcp-server.md` | Tool taxonomy, response envelope, sensitivity tiers, services | `src/moneybin/mcp/**`, `services/**` |
| `cli.md` | Typer patterns, error handling, command registration, icons | `src/moneybin/cli/**`, `main.py` |
| `testing.md` | Pytest patterns, fixtures, mocking strategy, DB test helpers | `tests/**`, `**/conftest.py` |
| `data-extraction.md` | Incremental sync, dedup, parameter design, new data sources | `extractors/**`, `connectors/**`, `loaders/**` |
| `identifiers.md` | Content hashes, truncated UUIDs, source IDs, semantic slugs | `src/moneybin/**/*.py`, `sqlmesh/models/**` |
