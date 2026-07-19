"""Registry <-> DuckDB catalog completeness checks.

These tests are the forcing function: a column added to ``core.*`` or
``app.*`` without a ``CLASSIFICATION`` entry fails CI here, not in a
privacy incident.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.sql_lineage import reports_class_map
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass


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


def test_unresolved_is_never_declared_as_a_column_class() -> None:
    """``UNRESOLVED`` is the ABSENCE of a classification — never a declaration.

    Writing it into ``CLASSIFICATION`` or a ``@report(classes=…)`` map would
    make a column that nobody classified look classified, and both completeness
    checks — the two above, and the reports-side derivation verification —
    would then pass over the very gap they exist to surface. The class's
    docstring in ``taxonomy.py`` asserts this; this test is what makes the
    assertion true rather than aspirational.
    """
    declared = [
        f"{schema}.{table}.{col}"
        for (schema, table), cols in CLASSIFICATION.items()
        for col, dc in cols.items()
        if dc is DataClass.UNRESOLVED
    ]
    declared += [
        f"{schema}.{table}.{col}"
        for (schema, table), cols in reports_class_map().items()
        for col, dc in cols.items()
        if dc is DataClass.UNRESOLVED
    ]
    assert not declared, (
        "DataClass.UNRESOLVED declared as a column class:\n"
        + "\n".join(f"  {d}" for d in sorted(declared))
        + "\nIt is the fail-closed marker for columns lineage could not "
        "resolve. Classify these columns properly instead."
    )
