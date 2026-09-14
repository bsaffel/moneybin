"""Guard: EXPECTED_CORE_COLUMNS must match the test fixture DDL.

If a SQLMesh model gets a new column, both EXPECTED_CORE_COLUMNS (production
boot guard) and CORE_*_DDL strings (test fixtures) must be updated in
lockstep. This test fails if they diverge.
"""

from __future__ import annotations

import re

from sqlglot import exp
from sqlmesh.core.dialect import parse as sqlmesh_parse
from sqlmesh.core.model import SqlModel, load_sql_based_model

from moneybin.database import EXPECTED_CORE_COLUMNS, SQLMESH_ROOT
from tests.moneybin import db_helpers


def _columns_from_create_table(ddl: str) -> set[str]:
    """Extract column names from a CREATE TABLE DDL string.

    Assumes each column line starts with ``<name> <type>``. Skips
    keyword/structural lines (CREATE, opening/closing parens, table-level
    constraints).
    """
    return set(_ordered_columns_from_create_table(ddl))


def _ordered_columns_from_create_table(ddl: str) -> list[str]:
    """Same extraction as :func:`_columns_from_create_table`, order preserved."""
    cols: list[str] = []
    for line in ddl.splitlines():
        stripped = line.strip().rstrip(",")
        if not stripped or stripped.upper().startswith(("CREATE", "PRIMARY", "(", ")")):
            continue
        match = re.match(r"^(\w+)\s+\w", stripped)
        if match:
            cols.append(match.group(1))
    return cols


def test_expected_columns_match_fixture_ddl() -> None:
    """Parity guard: EXPECTED_CORE_COLUMNS == fixture DDL column sets."""
    fixture_ddls = {
        "core.dim_accounts": db_helpers.CORE_DIM_ACCOUNTS_DDL,
        "core.fct_balances_daily": db_helpers.CORE_FCT_BALANCES_DAILY_DDL,
    }
    assert set(fixture_ddls) == set(EXPECTED_CORE_COLUMNS), (
        f"Table coverage mismatch: fixtures={set(fixture_ddls)}, "
        f"expected={set(EXPECTED_CORE_COLUMNS)}"
    )
    for table, expected in EXPECTED_CORE_COLUMNS.items():
        ddl = fixture_ddls[table]
        fixture_cols = _columns_from_create_table(ddl)
        assert set(expected) == fixture_cols, (
            f"{table}: expected has {set(expected) - fixture_cols} extra, "
            f"fixture has {fixture_cols - set(expected)} extra. "
            f"Keep them in sync."
        )


def test_dim_accounts_fixture_matches_model_projection_order() -> None:
    """Order guard: the fixture DDL must match ``dim_accounts.sql``'s SELECT order.

    The set-based guard above catches a missing/extra column but not a
    transposition -- per ``.claude/rules/column-ordering.md``, the SQL
    projection is the one source of column order, and Rule A ordering in
    ``core``/``prep`` is otherwise review-enforced only (no live catalog
    needed to check it, but nothing did). ``dim_accounts`` is ``kind FULL``
    with real SQL text, so its true order is derivable with no database --
    parse it the same connectionless way ``report_class_derivation.py``
    parses reports/core view models, and keep the fixture in lockstep so a
    positional INSERT or ``SELECT *`` in a test exercises the same physical
    column order production does.

    ``core.fct_balances_daily`` has no equivalent check: it is a Python
    model (``fct_balances_daily.py``) with no SQL text for this technique to
    parse, so its fixture order stays review-enforced like the rest of
    ``core``.
    """
    model_path = SQLMESH_ROOT / "models" / "core" / "dim_accounts.sql"
    model = load_sql_based_model(
        sqlmesh_parse(model_path.read_text(), default_dialect="duckdb"),
        path=model_path,
        dialect="duckdb",
    )
    assert isinstance(model, SqlModel), (
        f"dim_accounts.sql loaded as {type(model).__name__}, expected SqlModel"
    )
    query = model.query
    assert isinstance(query, exp.Query), (
        f"dim_accounts.sql query is {type(query).__name__}, expected a "
        "resolvable SQL AST"
    )
    model_order = list(query.named_selects)
    fixture_order = _ordered_columns_from_create_table(db_helpers.CORE_DIM_ACCOUNTS_DDL)
    assert model_order == fixture_order, (
        "core.dim_accounts fixture DDL column order has drifted from the "
        f"model's SELECT order: model={model_order}, fixture={fixture_order}"
    )
