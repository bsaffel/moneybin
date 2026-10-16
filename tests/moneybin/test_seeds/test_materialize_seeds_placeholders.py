"""Regression: materializing seeds over placeholder tables must not warn.

Every ``Database`` open creates empty ``seeds.*`` placeholder tables so the dim
views resolve before SQLMesh has run. SQLMesh promotes each seed as a VIEW, and
when it finds a table under that name it drops it and logs ``Target data object
... is a table and not a view`` — which surfaced on every ``moneybin demo``.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.seeds import SEED_PLACEHOLDERS, materialize_seeds
from moneybin.tables import SEED_CATEGORIES

pytestmark = pytest.mark.integration


def _table_types(db: Database) -> dict[str, str]:
    rows = db.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'seeds'"
    ).fetchall()
    return dict(rows)


@pytest.mark.slow
def test_materialize_seeds_replaces_placeholders_without_warning(
    db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    before = _table_types(db)
    assert all(before.get(t.name) == "BASE TABLE" for t in SEED_PLACEHOLDERS), before

    materialize_seeds(db)

    type_mismatch = [
        r.getMessage() for r in caplog.records if "and not a" in r.getMessage()
    ]
    assert not type_mismatch, type_mismatch
    after = _table_types(db)
    assert all(after.get(t.name) == "VIEW" for t in SEED_PLACEHOLDERS), after
    sql = f"SELECT COUNT(*) FROM {SEED_CATEGORIES.full_name}"  # noqa: S608  # TableRef constant
    assert db.execute(sql).fetchone() != (0,)
