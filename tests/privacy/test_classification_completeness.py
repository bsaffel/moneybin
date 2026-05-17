"""Registry <-> DuckDB catalog completeness checks.

These tests are the forcing function: a column added to ``core.*`` or
``app.*`` without a ``CLASSIFICATION`` entry fails CI here, not in a
privacy incident.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.taxonomy import CLASSIFICATION


def _live_columns(db: Database) -> set[tuple[str, str, str]]:
    rows = db.execute(
        """
        SELECT schema_name, table_name, column_name
        FROM duckdb_columns()
        WHERE schema_name IN ('core', 'app')
        """
    ).fetchall()
    return {(str(s), str(t), str(c)) for s, t, c in rows}


def _registered_columns() -> set[tuple[str, str, str]]:
    out: set[tuple[str, str, str]] = set()
    for (schema, table), cols in CLASSIFICATION.items():
        for col in cols:
            out.add((schema, table, col))
    return out


def test_every_live_column_is_classified(populated_db: Database) -> None:
    missing = _live_columns(populated_db) - _registered_columns()
    if missing:
        pretty = "\n".join(
            f"  {schema}.{table}.{col}" for schema, table, col in sorted(missing)
        )
        pytest.fail(
            "Columns in core.* / app.* without a CLASSIFICATION entry:\n"
            f"{pretty}\n"
            "Add each column to src/moneybin/privacy/taxonomy.py "
            "CLASSIFICATION dict with the appropriate DataClass."
        )


def test_no_stale_registry_entries(populated_db: Database) -> None:
    stale = _registered_columns() - _live_columns(populated_db)
    if stale:
        pretty = "\n".join(
            f"  {schema}.{table}.{col}" for schema, table, col in sorted(stale)
        )
        pytest.fail(
            "CLASSIFICATION entries that no longer exist in the live "
            f"schema:\n{pretty}\n"
            "Remove each from src/moneybin/privacy/taxonomy.py."
        )
