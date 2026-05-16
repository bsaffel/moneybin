"""Guard: EXPECTED_CORE_COLUMNS must match the test fixture DDL.

If a SQLMesh model gets a new column, both EXPECTED_CORE_COLUMNS (production
boot guard) and CORE_*_DDL strings (test fixtures) must be updated in
lockstep. This test fails if they diverge.
"""

from __future__ import annotations

import re

from moneybin.database import EXPECTED_CORE_COLUMNS
from tests.moneybin import db_helpers


def _columns_from_create_table(ddl: str) -> set[str]:
    """Extract column names from a CREATE TABLE DDL string.

    Assumes each column line starts with ``<name> <type>``. Skips
    keyword/structural lines (CREATE, opening/closing parens, table-level
    constraints).
    """
    cols: set[str] = set()
    for line in ddl.splitlines():
        stripped = line.strip().rstrip(",")
        if not stripped or stripped.upper().startswith(("CREATE", "PRIMARY", "(", ")")):
            continue
        match = re.match(r"^(\w+)\s+\w", stripped)
        if match:
            cols.add(match.group(1))
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
