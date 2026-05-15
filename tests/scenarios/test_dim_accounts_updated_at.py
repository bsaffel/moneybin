"""Verify core.dim_accounts.updated_at uses the per-row GREATEST formula.

The column must reflect the latest of all per-row input timestamps contributing
to a row's current values (raw loaded_at and app.account_settings.updated_at),
not CURRENT_TIMESTAMP of the SQLMesh apply. See
docs/specs/core-updated-at-convention.md.
"""

from __future__ import annotations

import pytest

from moneybin.database import sqlmesh_context
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_updated_at_equals_loaded_at_when_no_settings() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        rows = db.execute(
            """
            SELECT a.account_id, a.updated_at, a.loaded_at
            FROM core.dim_accounts AS a
            LEFT JOIN app.account_settings AS s USING (account_id)
            WHERE s.account_id IS NULL
            """
        ).fetchall()

    assert rows, "scenario must produce accounts with no settings row"
    for account_id, updated_at, loaded_at in rows:
        assert updated_at == loaded_at, (
            f"{account_id}: updated_at={updated_at} should equal loaded_at={loaded_at}"
        )


@pytest.mark.scenarios
@pytest.mark.slow
def test_updated_at_reflects_settings_when_settings_present() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        account_row = db.execute(
            "SELECT account_id FROM core.dim_accounts ORDER BY account_id LIMIT 1"
        ).fetchone()
        assert account_row is not None, "scenario produced no accounts"
        account_id = account_row[0]

        # Upsert a settings row with a fresh timestamp.
        # NOW() inside ON CONFLICT DO UPDATE — DuckDB treats CURRENT_TIMESTAMP
        # there as an identifier, not a function call. Matches account_service.upsert.
        db.execute(
            """
            INSERT INTO app.account_settings (account_id, updated_at)
            VALUES (?, NOW())
            ON CONFLICT (account_id) DO UPDATE SET updated_at = NOW()
            """,
            [account_id],
        )

        settings_updated_at = db.execute(
            "SELECT updated_at FROM app.account_settings WHERE account_id = ?",
            [account_id],
        ).fetchone()
        assert settings_updated_at is not None
        settings_ts = settings_updated_at[0]

        # Force-restate dim_accounts so it picks up the new settings row.
        # A plain plan() is a no-op when no SQLMesh-tracked upstream changed —
        # app.account_settings is outside the model graph.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=["core.dim_accounts"],
                auto_apply=True,
                no_prompts=True,
            )

        dim_row = db.execute(
            "SELECT updated_at FROM core.dim_accounts WHERE account_id = ?",
            [account_id],
        ).fetchone()
        assert dim_row is not None
        dim_ts = dim_row[0]

    assert dim_ts == settings_ts, (
        f"dim_accounts.updated_at={dim_ts} should equal "
        f"app.account_settings.updated_at={settings_ts}"
    )
