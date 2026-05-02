# Feature: MCP SQL Schema Discoverability

## Status
implemented

## Goal

Give the MCP-connected LLM enough schema context to write accurate `sql_query` calls on the first try, without spending round-trips on catalog reconnaissance. Restrict the curated surface to a small set of consumer-facing **interface tables** so the LLM doesn't reach into `raw`, `prep`, or `meta` schemas by default.

## Background

The `sql_query` tool (`src/moneybin/mcp/tools/sql.py`) accepts read-only SQL but ships no schema context. Today, the LLM either guesses table/column names or runs 1-3 introspection queries (`SHOW TABLES`, `DESCRIBE …`) before each non-trivial request.

Existing assets this design builds on:

- **`tables.py`** is the authoritative registry of `TableRef` constants.
- **DuckDB catalog already carries column docs.** SQLMesh's `register_comments` propagates the inline `/* ... */` blocks on each model's final SELECT into `duckdb_columns().comment`. Schema DDL files apply table/column comments via `schema.py:_apply_comments()` on app startup. So `duckdb_tables()` and `duckdb_columns()` are the source of truth for both structure and prose — no parallel YAML to maintain.
- **MCP resources** (`src/moneybin/mcp/resources.py`) already exist for `moneybin://status`, `moneybin://accounts`, `moneybin://privacy`. New resource follows the same pattern.
- **Architecture rule**: per `mcp-architecture.md`, consumers read from **core** (analytics) and **app** (user-authored state). Raw and prep are implementation details.

## Requirements

1. The LLM connected via MCP can discover, in one resource read, every table it should query, including: schema-qualified name, table purpose, every column with type and prose comment, and 2-3 representative example queries per table.
2. Only the curated **interface tables** appear in the resource. Other schemas (`raw`, `prep`, `meta`, `seeds`) are reachable only via explicit catalog queries through `sql_query`.
3. The interface-table set is declared **once**, at the `TableRef` definition site in `tables.py`. There is no parallel list to keep in sync.
4. The resource includes a "beyond the interface" footer that names the other schemas with one-line purposes and provides a sample catalog query so a power user (or the LLM, when explicitly asked) can spelunk further without consulting external docs.
5. The `sql_query` tool description includes a one-line pointer to the resource, as a fallback for clients that do not auto-load resources.
6. A startup-time / test-time assertion catches **stale-entry drift**: every `INTERFACE_TABLES` member must exist in `duckdb_tables()`.
7. Example queries are tested end-to-end: every example query in the catalog must parse and execute against a seeded test database without error.
8. Schema metadata is `low` sensitivity — no consent gate.

## Data Model

No schema changes. The feature is read-only against existing catalog tables (`duckdb_tables`, `duckdb_columns`).

The `TableRef` `NamedTuple` in `src/moneybin/tables.py` gains one field:

```python
class TableRef(NamedTuple):
    schema: str
    name: str
    audience: Literal["interface", "internal"] = "internal"

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.name}"
```

Interface tables are tagged at the call site:

```python
FCT_TRANSACTIONS = TableRef("core", "fct_transactions", audience="interface")
DIM_ACCOUNTS = TableRef("core", "dim_accounts", audience="interface")
BRIDGE_TRANSFERS = TableRef("core", "bridge_transfers", audience="interface")

CATEGORIES = TableRef("app", "categories", audience="interface")
BUDGETS = TableRef("app", "budgets", audience="interface")
TRANSACTION_NOTES = TableRef("app", "transaction_notes", audience="interface")
MERCHANTS = TableRef("app", "merchants", audience="interface")
CATEGORIZATION_RULES = TableRef("app", "categorization_rules", audience="interface")
TRANSACTION_CATEGORIES = TableRef("app", "transaction_categories", audience="interface")
```

All other `TableRef` constants stay on the default (`"internal"`).

A module-level helper derives the interface tuple from the constants defined in the module:

```python
INTERFACE_TABLES: tuple[TableRef, ...] = tuple(
    t for t in _all_table_refs() if t.audience == "interface"
)
```

`_all_table_refs()` introspects the module's globals (`vars(sys.modules[__name__])`) and filters for `TableRef` instances. This avoids a hand-maintained second list.

## Resource Document Shape

`moneybin://schema` returns pretty-printed JSON:

```json
{
  "version": 1,
  "generated_at": "2026-05-01T18:30:00Z",
  "conventions": {
    "amount_sign": "negative = expense, positive = income",
    "currency": "DECIMAL(18,2); ISO 4217 codes in currency_code columns",
    "dates": "DATE type; transaction_date is the canonical posting date",
    "ids": "Deterministic SHA-256 truncated to 16 hex chars; see core.fct_transactions.transaction_id"
  },
  "tables": [
    {
      "name": "core.fct_transactions",
      "purpose": "Canonical transactions fact view; reads from the deduplicated merged layer with categorization and merchant joins; negative amount = expense, positive = income",
      "columns": [
        {"name": "transaction_id", "type": "VARCHAR", "nullable": false, "comment": "Gold key: deterministic SHA-256 hash, unique per real-world transaction"},
        {"name": "amount", "type": "DECIMAL(18,2)", "nullable": false, "comment": "Transaction amount; negative = expense, positive = income"}
      ],
      "examples": [
        {
          "question": "Total spending by category last month",
          "sql": "SELECT category, SUM(amount_absolute) AS total FROM core.fct_transactions WHERE transaction_direction = 'expense' AND transaction_year_month = STRFTIME(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m') GROUP BY category ORDER BY total DESC"
        }
      ]
    }
  ],
  "beyond_the_interface": {
    "note": "The tables above are the curated query surface. Other schemas exist for raw ingest (raw), staging (prep), provenance (meta), and seed data (seeds). Use them only when the curated tables cannot answer the question.",
    "catalog_query": "SELECT table_schema, table_name, comment FROM duckdb_tables() WHERE table_schema NOT IN ('main', 'pg_catalog') ORDER BY 1, 2"
  }
}
```

Key choices:

- **Top-level `conventions`** captures cross-table rules once; column comments stay focused on what makes that column distinct.
- **`purpose`** is pulled directly from `duckdb_tables().comment` (already populated from the `/* */` block above each `MODEL()`).
- **`columns`** is a structured projection of `duckdb_columns()` filtered to interface tables — no manual transcription.
- **`examples`** are hand-authored. They live in a Python dict in `services/schema_catalog.py`, keyed by `TableRef.full_name`. Each table gets 2-3 examples chosen to teach **idioms** (period grouping via `transaction_year_month`, sign filtering via `transaction_direction`, joins between fact and dimension tables) — not exhaustive coverage.

## Implementation Plan

### Files to Create

- `src/moneybin/services/schema_catalog.py` — `Example` dataclass, `EXAMPLES: dict[str, list[Example]]`, `build_schema_doc() -> dict`, conventions block.
- `tests/moneybin/test_services/test_schema_catalog.py` — see Testing Strategy.

### Files to Modify

- `src/moneybin/tables.py` — add `audience` field to `TableRef`, mark interface tables, add `INTERFACE_TABLES` derivation helper.
- `src/moneybin/mcp/resources.py` — register `moneybin://schema` resource (~10 lines, calls `build_schema_doc()` and `json.dumps`).
- `src/moneybin/mcp/tools/sql.py` — append a single line to `sql_query`'s docstring and the registration description: *"For schema, columns, and example queries, read resource `moneybin://schema`."*
- `sqlmesh/models/core/fct_transactions.sql`, `sqlmesh/models/core/dim_accounts.sql`, `sqlmesh/models/core/bridge_transfers.sql` — add a one-line pointer comment near the top: `/* Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict) */`.
- `src/moneybin/sql/schema/app_*.sql` files for each interface app table (e.g. `app_budgets.sql`, `app_categories.sql`, `app_merchants.sql`, `app_categorization_rules.sql`, `app_transaction_categories.sql`, `app_transaction_notes.sql`) — same pointer comment.
- `docs/followups.md` — append a section noting that examples could move to sibling `.examples.sql` files if drift becomes a real problem (see "Out of Scope" below).
- `docs/specs/INDEX.md` — add this spec to the **MCP** section.

### Key Decisions

- **No caching in v1.** `duckdb_tables()` / `duckdb_columns()` are catalog reads — microseconds. Add caching only if profiling shows it matters.
- **Single bundled resource, not per-table resources.** Total payload is ~2-4K tokens for ~9 tables / ~95 columns. One fetch covers every subsequent query in the session; per-table would force the LLM to repeatedly fetch `fct_transactions` (used in most queries) plus a second table per join.
- **JSON, not Markdown.** Structured output is sliceable for future tooling (e.g., a `sql_describe(table)` tool would call the same `build_schema_doc()` and project a single entry).
- **Drift coverage.** Two tests address two drift modes: a presence assertion catches stale `INTERFACE_TABLES` entries; an example-execution test catches column renames that examples missed. The "forgot to add a new interface table" case is not mechanically catchable — it relies on convention (a comment near the top of `tables.py`).
- **Power-user path is `sql_query` against the catalog**, not a parameterized resource. Adding `moneybin://schema/all` later is trivial if real demand emerges.
- **Sensitivity is `low`.** Schema metadata — table names, column names, structural comments — is not PII. No consent gate; audit log entry on each read like other resources.

## CLI Interface

None. The schema doc is consumed by the MCP layer. If a CLI surface is desired later, it would be a thin wrapper: `moneybin mcp schema [--json|--markdown]` calling `build_schema_doc()`.

## MCP Interface

**New resource:**

| URI | Sensitivity | Returns |
|---|---|---|
| `moneybin://schema` | low | JSON document describing every interface table, with conventions and example queries |

**Modified tool:**

| Tool | Change |
|---|---|
| `sql_query` | Description gains one line: *"For schema, columns, and example queries, read resource `moneybin://schema`."* No behavior change. |

## Testing Strategy

Three unit tests in `tests/moneybin/test_services/test_schema_catalog.py`:

1. **`test_interface_tables_present_in_catalog`** — initialize the test database, then assert every `TableRef` in `INTERFACE_TABLES` appears in `duckdb_tables()`. Catches stale-entry drift (table renamed/removed but constant still tagged `interface`).
2. **`test_examples_only_reference_interface_tables`** — every key in `EXAMPLES` must equal some `TableRef.full_name` from `INTERFACE_TABLES`. Catches orphan examples for tables that no longer exist or were never interface-tagged.
3. **`test_examples_parse_and_run`** — seed the test database (existing fixtures), iterate every example query in `EXAMPLES`, execute it via `db.execute(query).fetchall()`. Any failure (parse error, missing column, type mismatch) fails the test. Catches column-renamed-but-example-not-updated drift.

Plus one integration test in `tests/moneybin/test_mcp/test_resources.py` (or wherever existing resource tests live):

4. **`test_schema_resource_returns_valid_json_with_all_interface_tables`** — read the resource, parse the JSON, assert every `INTERFACE_TABLES.full_name` is present in `tables[].name`, assert `conventions` and `beyond_the_interface` keys exist.

## Dependencies

None. All required machinery (DuckDB catalog tables, FastMCP resources, `Database`, `TableRef`) is already in place.

## Out of Scope

- **Sibling `.examples.sql` files per model.** Considered and deferred; tracked in `docs/followups.md`. If example drift becomes a real maintenance problem (examples that reference dropped columns, examples that contradict model logic), revisit by parsing per-table sibling files at startup.
- **Per-table resources** (`moneybin://schema/<table>`). Not needed at current size; revisit if `INTERFACE_TABLES` grows past ~20 tables or column count past ~300.
- **`moneybin://schema/all`** (uncurated view). Power users get this via `sql_query` against the DuckDB catalog. Add only if real demand emerges.
- **Caching the schema doc.** Catalog reads are sub-millisecond; not worth the invalidation cost in v1.
- **Auto-extracting examples from existing test cases** (e.g., test_services queries). Would couple the LLM-facing surface to test internals; explicit hand-authored examples are clearer.
