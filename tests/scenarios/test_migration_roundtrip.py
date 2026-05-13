"""Scenario: migrations apply to the right schema; populated columns survive.

Two complementary test variants:

* ``test_migration_roundtrip_yaml`` runs the YAML scenario through
  ``run_scenario`` with the standard Tier 1 backfill — proving the
  post-migration state is correct in absolute terms.
* ``test_migration_roundtrip_preserves_row_counts`` drives the steps
  step-by-step via the ``scenario_env`` context manager, snapshotting
  ``core.fct_transactions`` and ``core.dim_accounts`` row counts before and
  after ``migrate``. Pre/post parity proves migrations didn't add or drop
  rows — the relative invariant the standard run can't observe.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database, get_database
from tests.scenarios._runner import (
    load_shipped_scenario,
    run_scenario,
    scenario_env,
)
from tests.scenarios._runner.steps import run_step
from tests.scenarios._tier1_backfill import tier1_backfill


def _row_count(db: Database, table: str) -> int:
    """Return ``COUNT(*)`` for a fully qualified ``schema.table``."""
    # Identifier is hard-coded in the test (only two known tables); no user
    # input flows in. Annotated for the linter.
    row = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # hard-coded test identifier
    return int(row[0]) if row else 0


@pytest.mark.scenarios
@pytest.mark.slow
def test_migration_roundtrip_yaml() -> None:
    """tiers: T1, T3-migration-applies-correctly."""
    scenario = load_shipped_scenario("migration-roundtrip")
    assert scenario is not None
    result = run_scenario(
        scenario,
        extra_assertions=tier1_backfill(scenario.setup),
    )
    assert result.passed, result.failure_summary()


@pytest.mark.scenarios
@pytest.mark.slow
def test_migration_roundtrip_preserves_row_counts() -> None:
    """tiers: T3-pre-post-parity. Migrating must not add or drop rows.

    Drives the pipeline step-by-step (rather than via ``run_scenario``) so
    we can snapshot row counts at the boundary around ``migrate``. The
    surrounding ``transform`` and ``match`` steps still execute so the
    fact tables exist on both sides of the snapshot.
    """
    scenario = load_shipped_scenario("migration-roundtrip")
    assert scenario is not None

    tracked_tables = ("core.fct_transactions", "core.dim_accounts")

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        db.close()
        db = get_database()
        pre = {tbl: _row_count(db, tbl) for tbl in tracked_tables}

        run_step("migrate", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)

        db.close()
        db = get_database()
        post = {tbl: _row_count(db, tbl) for tbl in tracked_tables}
        db.close()

    assert pre == post, f"row counts changed across migrate: pre={pre} post={post}"
    # Sanity: the snapshot is meaningful only if data actually exists.
    assert all(count > 0 for count in pre.values()), pre
