# MoneyBin

Personal financial data platform. Python + DuckDB + SQLMesh + Typer CLI + MCP server.

## Critical Rules

- **Package manager**: `uv` only. Never `pip install`, `uv pip install`, or `python -m`.  Use `uv add` and similar commands.
- **Linting/formatting**: Ruff (line length 88). Run `uv run ruff format . && uv run ruff check .` before committing.
- **Type checking**: Pyright (not mypy). Run `uv run pyright` on modified files.
- **Tests**: `uv run pytest tests/ -v`
- **Pre-commit checklist**: `uv run ruff format . && uv run ruff check . && uv run pyright && uv run pytest tests/`
- **SQL linting**: SQLFluff with DuckDB dialect. Config in `pyproject.toml` only (no `.sqlfluff` files).

## Library Preference

**DuckDB > Polars > Pandas.** Use Pandas only for external library compatibility (document why).

## Code Standards

- Type hints on all function parameters and return values. Modern syntax: `str | None`, `list[str]`.
- Google-style docstrings with Args/Returns/Raises.
- Catch specific exceptions, not bare `Exception`.
- Structured logging: `logger = logging.getLogger(__name__)` with appropriate levels.
- Triple-quoted strings (`"""..."""`) for inline SQL.
- Always include a reason for `# noqa:` or `# type: ignore` comments.
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

## Configuration

Use Pydantic Settings as single source of truth. Never hardcode paths or credentials.

```python
from moneybin.config import get_database_path, get_settings

conn = duckdb.connect(str(get_database_path()))
```

Env vars use `MONEYBIN_` prefix with `__` for nesting: `MONEYBIN_PLAID__CLIENT_ID`.

## Security

- `SecretStr` for passwords/API keys in Pydantic Settings.
- Parameterized SQL queries (never string interpolation).
- Subprocess commands as lists (`["cmd", "arg"]`), never `shell=True` with user input.
- Validate paths against traversal (`..`, absolute paths).
- Log detailed errors internally; return generic messages to users.
