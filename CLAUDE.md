# MoneyBin

Personal financial data platform. Python + DuckDB + SQLMesh + Typer CLI + MCP server.

## Design Philosophy

- **Production-grade quality.** Prefer design patterns that align with industry standards over shortcuts or hobby-project patterns. Features can be phased, but what ships should follow the same conventions as established open source tools.
- **Sync server is opaque.** When integrating with moneybin-server (optional sync service), the client communicates only with the server's API surface. External service providers are implementation details hidden behind the server.

## Critical Rules

- **Package manager**: `uv` only. Never `pip install`, `uv pip install`, or `python -m`.  Use `uv add` and similar commands.
- **Linting/formatting**: Ruff (line length 88). Run `make format && make lint` (or `uv run ruff format . && uv run ruff check .`) before committing.
- **Type checking**: Pyright (not mypy). Run `uv run pyright` on modified files.
- **Tests**: During development, run only the relevant file(s): `uv run pytest tests/path/to/test_file.py -v`. Before committing, run the full suite: `make test`.
- **Pre-commit checklist**: `make check test` — runs format, lint, type-check, and unit tests. Run this once before committing, not after every change.
- **SQL formatting**: `uv run sqlmesh -p sqlmesh format` (uses sqlglot, understands SQLMesh `MODEL()` syntax).
- **Check the docs first**: Before implementing any pattern involving a library (SQLMesh, DuckDB, Pydantic, etc.), check the authoritative library docs to confirm the correct API and behavior. Do not rely solely on training knowledge — APIs change and edge cases matter. Each `.claude/rules/*.md` file lists relevant doc URLs where applicable.

## Library Preference

**DuckDB > Polars > Pandas.** Use Pandas only for external library compatibility (document why).

## Code Standards

- Type hints on all function parameters and return values. Modern syntax: `str | None`, `list[str]`.
- Google-style docstrings with Args/Returns/Raises.
- Catch specific exceptions, not bare `Exception`.
- Structured logging: `logger = logging.getLogger(__name__)` with appropriate levels. **Always use f-strings in log messages** (e.g. `logger.info(f"Loaded {n} records")`). Never use `%s`/`%d`-style lazy formatting — it contradicts the project convention and bypasses the `SanitizedLogFormatter`'s pattern matching.
- Triple-quoted strings (`"""..."""`) for inline SQL.
- Always include a reason for `# noqa:` or `# type: ignore` comments. The reason goes inline after the rule code, e.g. `# noqa: S608  # building test input string, not executing SQL`.
- Acronyms use ALL CAPS in class names: `OFXExtractor`, `CSVReader`, `PDFExtractor` (follows stdlib convention like `HTTPServer`).

## Architecture: Data Layers

| Layer | Schema | Materialized | Purpose |
|-------|--------|-------------|---------|
| Raw | `raw` | Table | Untouched data from loaders (Python) |
| Staging | `prep` | View | Light cleaning, type casting (SQLMesh `stg_*`) |
| Core | `core` | Table | Canonical, deduplicated, multi-source |

### Key Principles

1. **One canonical table per entity** -- `dim_accounts`, `fct_transactions`, etc. All consumers read from core only.
2. **Multi-source union** -- Core models `UNION ALL` from every staging source with `source_system` column.
3. **Dedup in core** -- `ROW_NUMBER()` windows for duplicate records; mapping tables for cross-source dedup.
4. **Accounting sign convention** -- negative = expense, positive = income. Amounts are `DECIMAL(18,2)`, dates are `DATE`.
5. **Source-agnostic consumers** -- MCP server, CLI, etc. use core `TableRef` constants, never source-specific logic.

### Adding a New Data Source

1. Create staging models in `sqlmesh/models/prep/` (views in `prep` schema)
2. Add a CTE to the relevant core model and `UNION ALL` into the `all_*` CTE
3. No changes needed to consumers

## Specs & Implementation Tracking

Feature specs live in `docs/specs/`. The **[Spec Index](docs/specs/INDEX.md)** is the single source of truth for what's been designed, what's in progress, and what's shipped.

- **Before implementing a feature**, check `docs/specs/INDEX.md` to see if a spec exists and what its status is.
- **When starting implementation**, update the spec's status to `in-progress` (both in the spec file and in `INDEX.md`).
- **When implementation is complete**, update the spec's status to `implemented` (both in the spec file and in `INDEX.md`). See `.claude/rules/shipping.md` for README and public documentation updates.
- **When writing a new spec**, add it to the Active specs table in `INDEX.md`.
- **Observability wiring**: Every spec that touches application code must include metrics in its implementation plan. Define new metrics in `registry.py` where appropriate, and wire `@tracked` / `track_duration` / manual `.inc()` / `.observe()` / `.set()` calls at integration points. See `docs/specs/observability.md` for the instrumentation API and existing metric definitions in `src/moneybin/metrics/registry.py`.
- Statuses: `draft` → `ready` → `in-progress` → `implemented`.

## Configuration

All config lives in `src/moneybin/config.py` — one file, one `MoneyBinSettings` root. Pydantic Settings is the single source of truth. Never hardcode paths, credentials, **or tunable parameters**.

- **What belongs in config:** Any value a user or operator might want to change without editing source code — paths, limits, thresholds, algorithm parameters (e.g. Argon2 cost factors), timeouts, default lookback windows. If you catch yourself writing a magic number that controls behavior, ask: should this be in config?
- **What does NOT belong in config:** Mathematical constants, regex patterns, SQL keywords, internal type identifiers. Function parameter defaults that represent API surface (e.g. MCP tool `limit=` args the caller can override) are fine as defaults, not config fields.
- **Never use `os.getenv()` directly.** All environment variable reads go through Pydantic Settings. Raw `os.getenv()` calls outside `config.py` are a bug — they bypass validation, type coercion, and the `MONEYBIN_` prefix convention.
- **Adding a new config section:** Create a frozen `BaseModel` subclass in `config.py` and add it as a field on `MoneyBinSettings`. Follow the existing pattern (`DatabaseConfig`, `SyncConfig`, etc.).
- **Accessing config:** Import `get_settings()` — never instantiate `MoneyBinSettings` directly except in tests.
- **Sensitive values:** Use `SecretStore` (see [`privacy-data-protection.md`](docs/specs/privacy-data-protection.md)), not raw `os.getenv()` or plain `str` fields for secrets.
- **Env vars** use `MONEYBIN_` prefix with `__` for nesting: `MONEYBIN_DATABASE__PATH`.

```python
from moneybin.database import get_database

db = get_database()
db.execute("SELECT * FROM core.fct_transactions WHERE account_id = ?", [account_id])
```

**Never call `duckdb.connect()` directly.** The `Database` class (`src/moneybin/database.py`) is the sole entry point for all database access. It handles encryption key retrieval, encrypted file attachment, schema initialization, and migrations. See [`privacy-data-protection.md`](docs/specs/privacy-data-protection.md).

## Constants

Security-critical parameters (crypto cost factors, key lengths, salt sizes) must be defined once — either as module-level `_CONSTANTS` or as config fields on the relevant `*Config` class. Never duplicate across call sites; extract a shared helper if two functions need the same parameters.

## Security

- **Encryption at rest**: All DuckDB databases are encrypted with AES-256-GCM by default. The `Database` class handles key retrieval and encrypted attachment transparently. See [`privacy-data-protection.md`](docs/specs/privacy-data-protection.md) for threat model, key management, and CLI commands.
- `SecretStr` for passwords/API keys in Pydantic Settings.
- Subprocess commands as lists (`["cmd", "arg"]`), never `shell=True` with user input.
- Log detailed errors internally; return generic messages to users.
- **No PII or financial data in logs**: Never log account numbers, routing numbers, balances, transaction amounts, or full descriptions. Log record counts, IDs, and status codes instead. Use masked or truncated values if context is needed (e.g., `account ...1234`). A `SanitizedLogFormatter` provides runtime detection and masking as a safety net.
- **Parameterized SQL** with `?` placeholders for all values. Validate dynamic identifiers against allowlists (e.g., `TableRef` constants). See `.claude/rules/security.md` for DuckDB-specific patterns and test conventions.
