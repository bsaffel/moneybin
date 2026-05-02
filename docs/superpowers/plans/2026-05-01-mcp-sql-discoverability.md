# MCP SQL Schema Discoverability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `moneybin://schema` MCP resource that returns a curated, structured schema document (interface tables + columns + comments + example queries) so the LLM can write accurate `sql_query` calls without per-session DESCRIBE/SHOW reconnaissance.

**Architecture:** Tag interface tables on `TableRef` via a new `audience` field. A new `services/schema_catalog.py` derives the interface-table tuple from the registry, joins live catalog metadata (`duckdb_tables()`, `duckdb_columns()`) with hand-authored example queries, and emits a JSON-serializable dict. The resource is a thin wrapper.

**Tech Stack:** Python 3.12, DuckDB (catalog reads), FastMCP (resource registration), Pydantic-free dataclasses, pytest.

**Spec:** `docs/specs/mcp-sql-discoverability.md`

---

## File Map

**Modify:**
- `src/moneybin/tables.py` — add `audience` to `TableRef`, mark 9 interface tables, add `INTERFACE_TABLES` helper
- `src/moneybin/mcp/resources.py` — register `moneybin://schema`
- `src/moneybin/mcp/tools/sql.py` — append schema pointer to docstring + registration description
- `tests/moneybin/db_helpers.py` — extend core DDL with inline comments matching SQLMesh models
- `sqlmesh/models/core/{fct_transactions,dim_accounts,bridge_transfers}.sql` — pointer comment
- `src/moneybin/sql/schema/app_{categories,budgets,transaction_notes,merchants,categorization_rules,transaction_categories}.sql` — pointer comment
- `docs/followups.md` — add co-location followup note

**Create:**
- `src/moneybin/services/schema_catalog.py` — `Example` dataclass, `EXAMPLES` dict, `build_schema_doc()`, `CONVENTIONS`
- `tests/moneybin/test_services/test_schema_catalog.py` — drift + structure + example-execution tests

---

## Task 1: Add `audience` to `TableRef` and derive `INTERFACE_TABLES`

**Files:**
- Modify: `src/moneybin/tables.py`
- Test: `tests/moneybin/test_tables.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/moneybin/test_tables.py`:

```python
"""Tests for the TableRef registry and INTERFACE_TABLES derivation."""

from __future__ import annotations

from moneybin.tables import (
    BRIDGE_TRANSFERS,
    BUDGETS,
    CATEGORIES,
    CATEGORIZATION_RULES,
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    INTERFACE_TABLES,
    MERCHANTS,
    OFX_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
    TRANSACTION_NOTES,
    TableRef,
)

EXPECTED_INTERFACE = {
    "core.fct_transactions",
    "core.dim_accounts",
    "core.bridge_transfers",
    "app.categories",
    "app.budgets",
    "app.transaction_notes",
    "app.merchants",
    "app.categorization_rules",
    "app.transaction_categories",
}


def test_audience_defaults_to_internal() -> None:
    t = TableRef("foo", "bar")
    assert t.audience == "internal"


def test_interface_tables_set_matches_expected() -> None:
    full_names = {t.full_name for t in INTERFACE_TABLES}
    assert full_names == EXPECTED_INTERFACE


def test_interface_tables_all_carry_interface_audience() -> None:
    for t in INTERFACE_TABLES:
        assert t.audience == "interface"


def test_internal_tables_excluded_from_interface() -> None:
    assert OFX_TRANSACTIONS not in INTERFACE_TABLES
    assert OFX_TRANSACTIONS.audience == "internal"


def test_full_name_unchanged() -> None:
    assert FCT_TRANSACTIONS.full_name == "core.fct_transactions"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_tables.py -v`
Expected: ImportError on `INTERFACE_TABLES`, or AttributeError on `audience`.

- [ ] **Step 3: Implement the change**

Replace the contents of `src/moneybin/tables.py` with:

```python
"""Table registry — single source of truth for schema-qualified table names.

All consumers (MCP server, CLI, services) import table constants from here.
"""

from __future__ import annotations

import sys
from typing import Literal, NamedTuple

# When adding a new TableRef constant: if it should be visible to MCP
# clients via the curated `moneybin://schema` resource, pass
# audience="interface". Otherwise it stays internal (default).


class TableRef(NamedTuple):
    """Reference to a database table with schema and name."""

    schema: str
    name: str
    audience: Literal["interface", "internal"] = "internal"

    @property
    def full_name(self) -> str:
        """Schema-qualified table name for use in SQL queries."""
        return f"{self.schema}.{self.name}"


# -- Core layer (canonical tables built by SQLMesh transforms) --
DIM_ACCOUNTS = TableRef("core", "dim_accounts", audience="interface")
FCT_TRANSACTIONS = TableRef("core", "fct_transactions", audience="interface")
BRIDGE_TRANSFERS = TableRef("core", "bridge_transfers", audience="interface")

# -- Raw tables (used until core models are built for these entities) --
OFX_ACCOUNTS = TableRef("raw", "ofx_accounts")
OFX_TRANSACTIONS = TableRef("raw", "ofx_transactions")
OFX_BALANCES = TableRef("raw", "ofx_balances")
OFX_INSTITUTIONS = TableRef("raw", "ofx_institutions")
W2_FORMS = TableRef("raw", "w2_forms")

# -- Raw tabular tables (replaces csv_* tables) --
TABULAR_TRANSACTIONS = TableRef("raw", "tabular_transactions")
TABULAR_ACCOUNTS = TableRef("raw", "tabular_accounts")
IMPORT_LOG = TableRef("raw", "import_log")

# -- App tables (application-managed data) --
TRANSACTION_CATEGORIES = TableRef("app", "transaction_categories", audience="interface")
BUDGETS = TableRef("app", "budgets", audience="interface")
TRANSACTION_NOTES = TableRef("app", "transaction_notes", audience="interface")
CATEGORIES = TableRef("app", "categories", audience="interface")
USER_CATEGORIES = TableRef("app", "user_categories")
CATEGORY_OVERRIDES = TableRef("app", "category_overrides")
MERCHANTS = TableRef("app", "merchants", audience="interface")
CATEGORIZATION_RULES = TableRef("app", "categorization_rules", audience="interface")
PROPOSED_RULES = TableRef("app", "proposed_rules")
RULE_DEACTIVATIONS = TableRef("app", "rule_deactivations")
SCHEMA_MIGRATIONS = TableRef("app", "schema_migrations")
VERSIONS = TableRef("app", "versions")

# -- App tabular tables --
TABULAR_FORMATS = TableRef("app", "tabular_formats")

# -- App matching tables --
MATCH_DECISIONS = TableRef("app", "match_decisions")
SEED_SOURCE_PRIORITY = TableRef("app", "seed_source_priority")

# -- Seed tables (materialized by SQLMesh from CSV) --
SEED_CATEGORIES = TableRef("seeds", "categories")

# -- Prep / staging views (built by SQLMesh transforms) --
INT_TRANSACTIONS_MATCHED = TableRef("prep", "int_transactions__matched")

# -- Meta schema (cross-source provenance + lineage) --
FCT_TRANSACTION_PROVENANCE = TableRef("meta", "fct_transaction_provenance")

# -- Synthetic tables (created on demand by the generator) --
GROUND_TRUTH = TableRef("synthetic", "ground_truth")


def _all_table_refs() -> tuple[TableRef, ...]:
    """Collect every TableRef constant defined at module scope.

    Walks this module's globals so the interface set is derived from
    the constant declarations rather than maintained as a parallel list.
    """
    module = sys.modules[__name__]
    return tuple(
        value for value in vars(module).values() if isinstance(value, TableRef)
    )


INTERFACE_TABLES: tuple[TableRef, ...] = tuple(
    t for t in _all_table_refs() if t.audience == "interface"
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_tables.py -v`
Expected: 5 passed.

- [ ] **Step 5: Run pyright to confirm types**

Run: `uv run pyright src/moneybin/tables.py tests/moneybin/test_tables.py`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/tables.py tests/moneybin/test_tables.py
git commit -m "Tag interface tables on TableRef and derive INTERFACE_TABLES"
```

---

## Task 2: Scaffold `services/schema_catalog.py` with conventions and Example dataclass

**Files:**
- Create: `src/moneybin/services/schema_catalog.py`
- Create: `tests/moneybin/test_services/test_schema_catalog.py`

- [ ] **Step 1: Write the failing test**

Create `tests/moneybin/test_services/test_schema_catalog.py`:

```python
"""Tests for the schema catalog service."""

from __future__ import annotations

from moneybin.services.schema_catalog import (
    CONVENTIONS,
    EXAMPLES,
    Example,
)
from moneybin.tables import INTERFACE_TABLES


def test_conventions_has_required_keys() -> None:
    assert set(CONVENTIONS.keys()) == {
        "amount_sign",
        "currency",
        "dates",
        "ids",
    }


def test_example_dataclass_shape() -> None:
    ex = Example(question="q?", sql="SELECT 1")
    assert ex.question == "q?"
    assert ex.sql == "SELECT 1"


def test_examples_only_reference_interface_tables() -> None:
    interface_names = {t.full_name for t in INTERFACE_TABLES}
    for table_name in EXAMPLES.keys():
        assert table_name in interface_names, (
            f"EXAMPLES key {table_name!r} is not an interface table"
        )


def test_every_interface_table_has_at_least_one_example() -> None:
    interface_names = {t.full_name for t in INTERFACE_TABLES}
    missing = interface_names - set(EXAMPLES.keys())
    assert not missing, f"Interface tables missing examples: {sorted(missing)}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_services/test_schema_catalog.py -v`
Expected: ImportError (`schema_catalog` does not exist).

- [ ] **Step 3: Create the module**

Create `src/moneybin/services/schema_catalog.py`:

```python
"""Schema catalog service — produces the LLM-facing schema document.

Joins live DuckDB catalog metadata (table/column types and comments) with
hand-authored example queries, filtered to the curated interface tables
declared in `moneybin.tables`.
"""

from __future__ import annotations

from dataclasses import dataclass

CONVENTIONS: dict[str, str] = {
    "amount_sign": "negative = expense, positive = income",
    "currency": "DECIMAL(18,2); ISO 4217 codes in currency_code columns",
    "dates": "DATE type; transaction_date is the canonical posting date",
    "ids": (
        "Deterministic SHA-256 truncated to 16 hex chars; "
        "see core.fct_transactions.transaction_id"
    ),
}


@dataclass(frozen=True)
class Example:
    """A single example query for a table."""

    question: str
    sql: str


EXAMPLES: dict[str, list[Example]] = {
    "core.fct_transactions": [
        Example(
            question="Total spending by category last month",
            sql=(
                "SELECT category, SUM(amount_absolute) AS total "
                "FROM core.fct_transactions "
                "WHERE transaction_direction = 'expense' "
                "AND transaction_year_month = "
                "STRFTIME(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m') "
                "GROUP BY category ORDER BY total DESC"
            ),
        ),
        Example(
            question="Transactions for an account in a date range",
            sql=(
                "SELECT transaction_date, description, amount, category "
                "FROM core.fct_transactions "
                "WHERE account_id = ? "
                "AND transaction_date BETWEEN ? AND ? "
                "ORDER BY transaction_date DESC"
            ),
        ),
        Example(
            question="Monthly spending trend (last 12 months)",
            sql=(
                "SELECT transaction_year_month, "
                "SUM(amount_absolute) AS total_spent "
                "FROM core.fct_transactions "
                "WHERE transaction_direction = 'expense' "
                "AND transaction_date >= CURRENT_DATE - INTERVAL 12 MONTH "
                "GROUP BY transaction_year_month "
                "ORDER BY transaction_year_month"
            ),
        ),
    ],
    "core.dim_accounts": [
        Example(
            question="List all accounts with their institution",
            sql=(
                "SELECT account_id, account_type, institution_name, source_type "
                "FROM core.dim_accounts ORDER BY institution_name, account_type"
            ),
        ),
        Example(
            question="Join accounts to transactions to label by institution",
            sql=(
                "SELECT a.institution_name, COUNT(*) AS txn_count, "
                "SUM(t.amount_absolute) AS total_volume "
                "FROM core.fct_transactions t "
                "JOIN core.dim_accounts a USING (account_id) "
                "GROUP BY a.institution_name ORDER BY total_volume DESC"
            ),
        ),
    ],
    "core.bridge_transfers": [
        Example(
            question="Confirmed transfer pairs in the last 90 days",
            sql=(
                "SELECT b.transfer_id, b.transfer_date, b.amount_absolute, "
                "b.from_account_id, b.to_account_id "
                "FROM core.bridge_transfers b "
                "WHERE b.transfer_date >= CURRENT_DATE - INTERVAL 90 DAY "
                "ORDER BY b.transfer_date DESC"
            ),
        ),
    ],
    "app.categories": [
        Example(
            question="All active categories",
            sql=(
                "SELECT category_id, category, subcategory, description "
                "FROM app.categories "
                "WHERE is_active ORDER BY category, subcategory"
            ),
        ),
    ],
    "app.budgets": [
        Example(
            question="Active budgets with their target amounts",
            sql="SELECT * FROM app.budgets ORDER BY category",
        ),
    ],
    "app.transaction_notes": [
        Example(
            question="All notes on a specific transaction",
            sql=(
                "SELECT transaction_id, note, created_at "
                "FROM app.transaction_notes WHERE transaction_id = ? "
                "ORDER BY created_at"
            ),
        ),
    ],
    "app.merchants": [
        Example(
            question="Merchants with their canonical names",
            sql=(
                "SELECT merchant_id, canonical_name, raw_pattern "
                "FROM app.merchants ORDER BY canonical_name"
            ),
        ),
    ],
    "app.categorization_rules": [
        Example(
            question="Active categorization rules",
            sql=(
                "SELECT rule_id, merchant_pattern, category, subcategory, priority "
                "FROM app.categorization_rules "
                "WHERE is_active ORDER BY priority DESC"
            ),
        ),
    ],
    "app.transaction_categories": [
        Example(
            question="Per-transaction category assignments",
            sql=(
                "SELECT transaction_id, category, subcategory, categorized_by "
                "FROM app.transaction_categories ORDER BY assigned_at DESC LIMIT 100"
            ),
        ),
    ],
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_services/test_schema_catalog.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/schema_catalog.py tests/moneybin/test_services/test_schema_catalog.py
git commit -m "Add CONVENTIONS, Example dataclass, and EXAMPLES dict for interface tables"
```

---

## Task 3: Implement `build_schema_doc()` against the live catalog

**Files:**
- Modify: `src/moneybin/services/schema_catalog.py`
- Modify: `tests/moneybin/test_services/test_schema_catalog.py`
- Modify: `tests/moneybin/db_helpers.py` (add COMMENT ON DDL after CREATE)

- [ ] **Step 1: Extend `db_helpers` so test core tables carry table + column comments**

Comments must reach the catalog so `build_schema_doc()` can read them. Add a new helper to `tests/moneybin/db_helpers.py` (append to end of file):

```python
# Table and column comments for core tables — mirror the SQLMesh model
# headers and inline column comments. Applied separately because the
# minimal CREATE TABLE DDL above does not embed them.
CORE_TABLE_COMMENTS: dict[str, str] = {
    "core.fct_transactions": (
        "Canonical transactions fact view; reads from the deduplicated "
        "merged layer with categorization and merchant joins; "
        "negative amount = expense, positive = income"
    ),
    "core.dim_accounts": (
        "Canonical accounts dimension; one row per account across sources"
    ),
}

CORE_COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    "core.fct_transactions": {
        "transaction_id": (
            "Gold key: deterministic SHA-256 hash, unique per real-world transaction"
        ),
        "amount": "Transaction amount; negative = expense, positive = income",
        "transaction_direction": ("Derived from amount sign: expense, income, or zero"),
        "category": (
            "Spending category; from app.transaction_categories when "
            "categorized, else source value"
        ),
    },
    "core.dim_accounts": {
        "account_id": "Stable per-source account identifier",
        "institution_name": "Display name of the issuing institution",
    },
}


def apply_core_table_comments(database: Database) -> None:
    """Apply COMMENT ON TABLE/COLUMN for core test tables.

    Production comments are applied by SQLMesh's `register_comments`;
    tests need to mirror that for the schema catalog tests to see prose.
    """
    for table, comment in CORE_TABLE_COMMENTS.items():
        # noqa: S608 — string-built DDL; values are module constants, not user input
        database.execute(  # noqa: S608
            f"COMMENT ON TABLE {table} IS '{comment.replace(chr(39), chr(39) * 2)}'"
        )
    for table, cols in CORE_COLUMN_COMMENTS.items():
        for col, comment in cols.items():
            database.execute(  # noqa: S608
                f"COMMENT ON COLUMN {table}.{col} IS "
                f"'{comment.replace(chr(39), chr(39) * 2)}'"
            )
```

- [ ] **Step 2: Write the failing test for `build_schema_doc()`**

Append to `tests/moneybin/test_services/test_schema_catalog.py`:

```python
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.schema_catalog import build_schema_doc
from tests.moneybin.db_helpers import (
    apply_core_table_comments,
    create_core_tables_raw,
)


@pytest.fixture()
def schema_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Database with core tables created and comments applied."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        path=tmp_path / "schema.duckdb",
        secret_store=mock_store,
        encryption_key_alias="test",
    )
    database.connect()
    create_core_tables_raw(database)
    apply_core_table_comments(database)
    db_module._database = database
    try:
        yield database
    finally:
        db_module._database = None
        database.close()


def test_build_schema_doc_top_level_keys(schema_db: Database) -> None:
    doc = build_schema_doc()
    assert doc["version"] == 1
    assert "generated_at" in doc
    assert doc["conventions"]["amount_sign"].startswith("negative")
    assert isinstance(doc["tables"], list)
    assert "beyond_the_interface" in doc
    assert "catalog_query" in doc["beyond_the_interface"]


def test_build_schema_doc_includes_present_interface_tables(
    schema_db: Database,
) -> None:
    doc = build_schema_doc()
    names = {t["name"] for t in doc["tables"]}
    # The test DB only creates core.* via create_core_tables_raw; app tables
    # are absent, so build_schema_doc should silently skip them rather than
    # error. Core interface tables must be present.
    assert "core.fct_transactions" in names
    assert "core.dim_accounts" in names


def test_build_schema_doc_columns_carry_type_and_comment(
    schema_db: Database,
) -> None:
    doc = build_schema_doc()
    fct = next(t for t in doc["tables"] if t["name"] == "core.fct_transactions")
    cols_by_name = {c["name"]: c for c in fct["columns"]}
    assert "amount" in cols_by_name
    assert "DECIMAL" in cols_by_name["amount"]["type"].upper()
    assert "negative" in cols_by_name["amount"]["comment"].lower()


def test_build_schema_doc_includes_examples_for_present_tables(
    schema_db: Database,
) -> None:
    doc = build_schema_doc()
    fct = next(t for t in doc["tables"] if t["name"] == "core.fct_transactions")
    assert len(fct["examples"]) >= 1
    first = fct["examples"][0]
    assert "question" in first
    assert "sql" in first
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_services/test_schema_catalog.py -v`
Expected: ImportError on `build_schema_doc`.

- [ ] **Step 4: Implement `build_schema_doc()`**

Append to `src/moneybin/services/schema_catalog.py`:

```python
import logging
from datetime import UTC, datetime
from typing import Any

from moneybin.database import get_database
from moneybin.tables import INTERFACE_TABLES

logger = logging.getLogger(__name__)

_BEYOND_NOTE = (
    "The tables above are the curated query surface. Other schemas exist "
    "for raw ingest (raw), staging (prep), provenance (meta), and seed "
    "data (seeds). Use them only when the curated tables cannot answer "
    "the question."
)
_BEYOND_QUERY = (
    "SELECT table_schema, table_name, comment FROM duckdb_tables() "
    "WHERE table_schema NOT IN ('main', 'pg_catalog') ORDER BY 1, 2"
)


def build_schema_doc() -> dict[str, Any]:
    """Return the schema document for the LLM-facing catalog.

    Reads `duckdb_tables()` and `duckdb_columns()` for every interface
    table that exists in the live database; missing tables are silently
    skipped (the test/dev DB may not have every interface table).
    """
    db = get_database()

    interface_names = [t.full_name for t in INTERFACE_TABLES]
    placeholders = ",".join(["?"] * len(interface_names))
    table_rows = db.execute(
        f"""
        SELECT schema_name || '.' || table_name AS full_name,
               COALESCE(comment, '') AS comment
        FROM duckdb_tables()
        WHERE schema_name || '.' || table_name IN ({placeholders})
        ORDER BY schema_name, table_name
        """,
        interface_names,
    ).fetchall()

    tables: list[dict[str, Any]] = []
    for full_name, table_comment in table_rows:
        schema_name, table_name = full_name.split(".", 1)
        col_rows = db.execute(
            """
            SELECT column_name, data_type, is_nullable,
                   COALESCE(comment, '') AS comment
            FROM duckdb_columns()
            WHERE schema_name = ? AND table_name = ?
            ORDER BY column_index
            """,
            [schema_name, table_name],
        ).fetchall()
        tables.append({
            "name": full_name,
            "purpose": table_comment,
            "columns": [
                {
                    "name": name,
                    "type": dtype,
                    "nullable": bool(nullable),
                    "comment": comment,
                }
                for name, dtype, nullable, comment in col_rows
            ],
            "examples": [
                {"question": ex.question, "sql": ex.sql}
                for ex in EXAMPLES.get(full_name, [])
            ],
        })

    logger.info("Schema doc built: %d interface tables present", len(tables))

    return {
        "version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "conventions": dict(CONVENTIONS),
        "tables": tables,
        "beyond_the_interface": {
            "note": _BEYOND_NOTE,
            "catalog_query": _BEYOND_QUERY,
        },
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_services/test_schema_catalog.py -v`
Expected: 8 passed (4 new + 4 from Task 2).

- [ ] **Step 6: Type check**

Run: `uv run pyright src/moneybin/services/schema_catalog.py tests/moneybin/test_services/test_schema_catalog.py tests/moneybin/db_helpers.py`
Expected: 0 errors.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/services/schema_catalog.py tests/moneybin/test_services/test_schema_catalog.py tests/moneybin/db_helpers.py
git commit -m "Implement build_schema_doc() reading live DuckDB catalog"
```

---

## Task 4: Add presence-drift assertion + example execution test

**Files:**
- Modify: `tests/moneybin/test_services/test_schema_catalog.py`

- [ ] **Step 1: Append two drift tests**

```python
def test_interface_tables_present_in_catalog(schema_db: Database) -> None:
    """Stale-entry drift: an interface-tagged table is missing from the DB.

    Only checks tables the test DB actually creates (core.*); the assertion
    here is that nothing tagged interface in the *core* schema is missing.
    """
    from moneybin.tables import INTERFACE_TABLES

    rows = schema_db.execute(
        "SELECT schema_name || '.' || table_name FROM duckdb_tables()"
    ).fetchall()
    present = {r[0] for r in rows}
    missing_core = [
        t.full_name
        for t in INTERFACE_TABLES
        if t.schema == "core" and t.full_name not in present
    ]
    assert not missing_core, (
        f"INTERFACE_TABLES core entries missing from catalog: {missing_core}"
    )


def test_examples_parse_and_execute(schema_db: Database) -> None:
    """Examples must parse and execute against the live schema.

    Catches column-renamed-but-example-not-updated drift. Skips examples
    for tables not present in the test DB (app.* tables not seeded here).
    Parameterized examples (containing '?') are validated via PREPARE
    instead of execution.
    """
    rows = schema_db.execute(
        "SELECT schema_name || '.' || table_name FROM duckdb_tables()"
    ).fetchall()
    present = {r[0] for r in rows}
    for table_name, examples in EXAMPLES.items():
        if table_name not in present:
            continue
        for ex in examples:
            if "?" in ex.sql:
                schema_db.execute(f"PREPARE __probe AS {ex.sql}")
                schema_db.execute("DEALLOCATE __probe")
            else:
                schema_db.execute(ex.sql).fetchall()
```

- [ ] **Step 2: Run the new tests**

Run: `uv run pytest tests/moneybin/test_services/test_schema_catalog.py -v`
Expected: 10 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/moneybin/test_services/test_schema_catalog.py
git commit -m "Add drift tests for interface table presence and example execution"
```

---

## Task 5: Register `moneybin://schema` resource

**Files:**
- Modify: `src/moneybin/mcp/resources.py`
- Test: `tests/moneybin/test_mcp/test_schema_resource.py` (create)

- [ ] **Step 1: Inspect existing resource test patterns**

Run: `ls tests/moneybin/test_mcp/`
Read one existing resource-style test to mirror its fixture setup (e.g., `test_resources.py` if present, otherwise the closest analog). The new test should follow whatever DB-bootstrapping pattern the existing MCP tests use.

- [ ] **Step 2: Write the failing test**

Create `tests/moneybin/test_mcp/test_schema_resource.py`:

```python
"""Test the moneybin://schema resource."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.mcp.resources import resource_schema
from tests.moneybin.db_helpers import (
    apply_core_table_comments,
    create_core_tables_raw,
)


@pytest.fixture()
def mcp_db(tmp_path: Path) -> Generator[Database, None, None]:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        path=tmp_path / "mcp.duckdb",
        secret_store=mock_store,
        encryption_key_alias="test",
    )
    database.connect()
    create_core_tables_raw(database)
    apply_core_table_comments(database)
    db_module._database = database
    try:
        yield database
    finally:
        db_module._database = None
        database.close()


def test_resource_schema_returns_valid_json(mcp_db: Database) -> None:
    body = resource_schema()
    parsed = json.loads(body)
    assert parsed["version"] == 1
    assert "tables" in parsed
    assert "conventions" in parsed
    assert "beyond_the_interface" in parsed


def test_resource_schema_contains_core_interface_tables(
    mcp_db: Database,
) -> None:
    parsed = json.loads(resource_schema())
    names = {t["name"] for t in parsed["tables"]}
    assert "core.fct_transactions" in names
    assert "core.dim_accounts" in names
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_mcp/test_schema_resource.py -v`
Expected: ImportError on `resource_schema`.

- [ ] **Step 4: Add the resource to `src/moneybin/mcp/resources.py`**

After the existing `resource_privacy` definition (or anywhere among the existing resources), add:

```python
from moneybin.services.schema_catalog import build_schema_doc


@mcp.resource("moneybin://schema")
def resource_schema() -> str:
    """Curated schema for ad-hoc SQL: interface tables, columns, comments, example queries."""
    logger.info("Resource read: moneybin://schema")
    doc = build_schema_doc()
    return json.dumps(doc, indent=2, default=str)
```

(The `json` and `logger` imports already exist at the top of the file.)

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_mcp/test_schema_resource.py -v`
Expected: 2 passed.

- [ ] **Step 6: Type check + lint**

Run: `uv run pyright src/moneybin/mcp/resources.py && make lint`
Expected: 0 errors.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/mcp/resources.py tests/moneybin/test_mcp/test_schema_resource.py
git commit -m "Register moneybin://schema MCP resource"
```

---

## Task 6: Point `sql_query` at the schema resource

**Files:**
- Modify: `src/moneybin/mcp/tools/sql.py`

- [ ] **Step 1: Edit the docstring and registration description**

In `src/moneybin/mcp/tools/sql.py`, replace the `sql_query` docstring (lines 21-31) with:

```python
    """Execute a read-only SQL query against the database.

    Only SELECT, WITH, DESCRIBE, SHOW, PRAGMA, and EXPLAIN queries
    are allowed. Write operations and file-access functions are blocked.

    Use this for ad-hoc analysis not covered by other tools. Results
    are limited to the configured maximum row count.

    For schema, columns, and example queries, read resource
    `moneybin://schema` before composing non-trivial queries.

    Args:
        query: The SQL query to execute.
    """
```

And update the registration description in `register_sql_tools` to:

```python
"Execute a read-only SQL query against the database."

"Supports SELECT, WITH, DESCRIBE, SHOW, PRAGMA, EXPLAIN. "
("Read resource moneybin://schema for tables, columns, and example queries.",)
```

- [ ] **Step 2: Verify nothing else broke**

Run: `uv run pytest tests/moneybin/test_mcp/ -q`
Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/moneybin/mcp/tools/sql.py
git commit -m "Point sql_query description at moneybin://schema resource"
```

---

## Task 7: Add pointer comments to model and DDL files

**Files:**
- Modify: `sqlmesh/models/core/fct_transactions.sql`
- Modify: `sqlmesh/models/core/dim_accounts.sql`
- Modify: `sqlmesh/models/core/bridge_transfers.sql`
- Modify: `src/moneybin/sql/schema/app_categories.sql`
- Modify: `src/moneybin/sql/schema/app_budgets.sql`
- Modify: `src/moneybin/sql/schema/app_transaction_notes.sql`
- Modify: `src/moneybin/sql/schema/app_merchants.sql`
- Modify: `src/moneybin/sql/schema/app_categorization_rules.sql`
- Modify: `src/moneybin/sql/schema/app_transaction_categories.sql`

The pointer comment is a single line that sits **after** the existing table-level header `/* ... */` comment (so it doesn't get picked up as the table's `COMMENT ON TABLE` description by `register_comments` / `_apply_comments`).

For each SQLMesh model, insert the pointer between the existing header comment and the `MODEL(` line. Example for `fct_transactions.sql`:

```sql
/* Canonical transactions fact view; reads from the deduplicated merged layer
   with categorization and merchant joins; negative amount = expense, positive = income */
-- Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict)
MODEL (
  name core.fct_transactions,
  ...
);
```

For each app DDL file, insert the pointer between the existing header `/* ... */` and the `CREATE TABLE` statement. Example for `app_categories.sql`:

```sql
/* Spending category definitions; seeded from Plaid PFCv2 taxonomy and extended with user-defined categories */
-- Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict)
CREATE TABLE IF NOT EXISTS app.categories (
    ...
);
```

- [ ] **Step 1: Apply pointer to each of the 9 files above**

Use Edit tool per file. The pointer line is identical for all 9: `-- Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict)`. Place it on the line immediately after the closing `*/` of the existing header.

- [ ] **Step 2: Format the SQLMesh files**

Run: `uv run sqlmesh -p sqlmesh format`
Expected: no output, exit 0. (`sqlmesh format` may rewrite `--` into `/* */`; that's fine.)

- [ ] **Step 3: Verify schema/SQLMesh tests still pass**

Run: `uv run pytest tests/moneybin/test_services/ tests/moneybin/test_mcp/ -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add sqlmesh/models/core/*.sql src/moneybin/sql/schema/app_*.sql
git commit -m "Add LLM-examples pointer comment to interface table definitions"
```

---

## Task 8: Append followups note + bump spec status

**Files:**
- Modify: `docs/followups.md`
- Modify: `docs/specs/mcp-sql-discoverability.md`
- Modify: `docs/specs/INDEX.md`

- [ ] **Step 1: Append to `docs/followups.md`**

Add at the end of the file:

```markdown

## Schema examples co-location (post-`mcp-sql-discoverability`)

Example queries currently live in `src/moneybin/services/schema_catalog.py`
(`EXAMPLES` dict) with one-line pointer comments in each interface model
and DDL file. If example drift becomes a real maintenance problem
(examples that reference dropped columns, examples that contradict model
logic, examples that lag behind schema changes), revisit the **sibling
`.examples.sql`** approach: one file per table next to the model, parsed
at startup. See `docs/specs/mcp-sql-discoverability.md` Section "Out of
Scope" and the brainstorming session that produced it.
```

- [ ] **Step 2: Bump spec status to `in-progress`**

In `docs/specs/mcp-sql-discoverability.md`, change:

```markdown
## Status
draft
```

to:

```markdown
## Status
in-progress
```

In `docs/specs/INDEX.md`, change the status column for `mcp-sql-discoverability` from `draft` to `in-progress`.

- [ ] **Step 3: Commit**

```bash
git add docs/followups.md docs/specs/mcp-sql-discoverability.md docs/specs/INDEX.md
git commit -m "Add schema-examples co-location followup; bump spec to in-progress"
```

---

## Task 9: End-to-end verification

- [ ] **Step 1: Run the full pre-commit checklist**

Run: `make check test`
Expected: format, lint, type-check, and full test suite all green.

- [ ] **Step 2: Manually exercise the resource against a real DB**

Run a small one-off script to confirm the resource produces sensible output against the actual user database:

```bash
uv run python -c "
from moneybin.services.schema_catalog import build_schema_doc
import json
doc = build_schema_doc()
print(f'tables: {len(doc[\"tables\"])}')
for t in doc['tables']:
    print(f'  {t[\"name\"]}: {len(t[\"columns\"])} cols, {len(t[\"examples\"])} examples')
print('beyond:', doc['beyond_the_interface']['note'][:60], '...')
"
```

Expected: prints all 9 interface tables (or however many exist in the active DB) with non-zero column and example counts. App tables present in the active DB should appear; missing ones should silently be absent.

- [ ] **Step 3: Final review pass with `/simplify`**

Per `.claude/rules/shipping.md`, run `/simplify` before pushing to catch any reuse, quality, or efficiency issues that accumulated during implementation.

- [ ] **Step 4: Mark spec implemented and update README**

In `docs/specs/mcp-sql-discoverability.md`, change `## Status\nin-progress` → `## Status\nimplemented`.
In `docs/specs/INDEX.md`, change status to `implemented`.
In `README.md`, change the roadmap row icon from 📐 to ✅.

Commit:

```bash
git add docs/specs/mcp-sql-discoverability.md docs/specs/INDEX.md README.md
git commit -m "Mark MCP SQL schema discoverability as implemented"
```

- [ ] **Step 5: Push the branch and open PR**

```bash
git push -u origin feat/mcp-sql-schema-resource
```

Open the PR using the `commit-push-pr` skill or `gh pr create`. Title: `Add curated MCP schema resource for ad-hoc SQL`.
