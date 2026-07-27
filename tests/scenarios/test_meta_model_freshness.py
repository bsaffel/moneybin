"""Smoke test for meta.model_freshness.

Applies the SQLMesh transform pipeline using the idempotency-rerun scenario and
asserts the view's public column contract holds against real SQLMesh state.
This is the only layer that exercises the view's own derivation — the unit
tests stub ``meta.model_freshness`` as a table, so a column dropped from the
CTE chain would leave them green.
"""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_meta_model_freshness_returns_row_per_model() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        core_rows = db.execute(
            """
            SELECT model_name, last_changed_at, last_applied_at,
                   last_executed_at, model_kind
            FROM meta.model_freshness
            WHERE model_name LIKE 'core.%'
            ORDER BY model_name
            """
        ).fetchall()
        external_rows = db.execute(
            """
            SELECT model_name, last_executed_at, model_kind
            FROM meta.model_freshness
            WHERE model_name LIKE 'raw.%'
            ORDER BY model_name
            """
        ).fetchall()

    assert core_rows, "meta.model_freshness returned no core.* rows"
    for name, _changed, applied, executed, kind in core_rows:
        assert applied is not None, f"{name} has NULL last_applied_at"
        # The step above ran a full apply, so every core model was backfilled.
        assert executed is not None, f"{name} has NULL last_executed_at"
        assert kind is not None, f"{name} has NULL model_kind"

    # `external_models.yaml` declares the `raw.*` sources, and SQLMesh never
    # executes an EXTERNAL model — so it records no interval for one and
    # `last_executed_at` stays NULL. `TransformService` filters exactly this set
    # back out; were the kind to arrive wrong, that filter would silently start
    # counting sources that can never carry a stamp.
    assert external_rows, "meta.model_freshness returned no raw.* rows"
    for name, executed, kind in external_rows:
        assert kind == "EXTERNAL", f"{name} has model_kind {kind!r}, want EXTERNAL"
        assert executed is None, f"{name} is EXTERNAL but has last_executed_at"


@pytest.mark.scenarios
@pytest.mark.slow
def test_a_version_that_never_backfilled_reports_no_execution() -> None:
    """A recorded-but-unbuilt version must not inherit its predecessor's stamp.

    SQLMesh writes a plan's snapshot rows before backfilling them, so a plan
    interrupted in between leaves a model whose *current* version has no
    interval while the previous version's intervals survive. Intervals belong
    to a ``(name, version)`` pair — SQLMesh's own accessor is
    ``hydrate_with_intervals_by_version``, and ``_intervals.version`` is
    written as ``snapshot.version`` — so attributing any of a model's
    executions to whichever version is current reports content that never ran
    as freshly built. ``TransformService.freshness()`` reads this column to
    decide ``pending``, which makes that a fail-open in the staleness signal.
    """
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    strip_catalog = "REGEXP_REPLACE(REPLACE(name, '\"', ''), '^[^.]+\\.', '')"

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        built = db.execute(
            """
            SELECT model_name FROM meta.model_freshness
            WHERE model_name LIKE 'core.%' AND last_executed_at IS NOT NULL
            ORDER BY model_name LIMIT 1
            """
        ).fetchone()
        assert built is not None, "no built core model to perturb"
        model_name = str(built[0])

        # Stand in for a plan that recorded a new version and then failed
        # before backfilling it: a newer snapshot row for the same model, with
        # no matching row in `_intervals`. Cloning the model's own newest row
        # keeps every other column truthful, so the version is the only thing
        # under test.
        db.execute(
            f"""
            INSERT INTO sqlmesh._snapshots
            SELECT * REPLACE (
                'probe_unbuilt_identifier' AS identifier,
                'probe_unbuilt_version' AS version,
                (SELECT MAX(updated_ts) + 1000 FROM sqlmesh._snapshots) AS updated_ts
            )
            FROM sqlmesh._snapshots
            WHERE {strip_catalog} = ?
            ORDER BY updated_ts DESC LIMIT 1
            """,  # noqa: S608  # strip_catalog is a module-local literal, not input
            [model_name],
        )

        executed = db.execute(
            "SELECT last_executed_at FROM meta.model_freshness WHERE model_name = ?",
            [model_name],
        ).fetchone()

    assert executed is not None, f"{model_name} vanished from the view"
    assert executed[0] is None, (
        f"{model_name}'s current version has no interval, but the view reported "
        f"last_executed_at={executed[0]} — an execution borrowed from an older "
        "version, reporting never-built content as freshly built"
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_a_requested_restatement_does_not_count_as_an_execution() -> None:
    """A pending-restatement row records a request, not a completed rebuild.

    SQLMesh stamps every ``_intervals`` row with ``created_ts = now`` — see
    ``_interval_to_df`` — including the rows it writes for *requested*
    restatements. Those carry no data: they mark an interval as owed, and the
    real row lands only once the backfill succeeds. SQLMesh's own
    max-interval read path excludes them for exactly this reason
    (``is_pending_restatement.not_()``). Counted as executions here, a
    restatement that is requested after a raw landing and then fails during
    backfill leaves a stamp newer than the landing, so ``freshness()`` reports
    ``pending=False`` over data the failure left stale — the fail-open this
    column exists to close.
    """
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    strip_catalog = "REGEXP_REPLACE(REPLACE(name, '\"', ''), '^[^.]+\\.', '')"

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        built = db.execute(
            """
            SELECT model_name, last_executed_at FROM meta.model_freshness
            WHERE model_name LIKE 'core.%' AND last_executed_at IS NOT NULL
            ORDER BY model_name LIMIT 1
            """
        ).fetchone()
        assert built is not None, "no built core model to perturb"
        model_name, before = str(built[0]), built[1]

        # Stand in for `transform restate --model <name>` whose backfill then
        # failed. Cloning the model's own newest prod interval keeps name and
        # version truthful — so this fixture can only be caught by the
        # pending-restatement filter, never by the (name, version) join — and
        # mirrors how SQLMesh writes the row: identifier and dev_version NULL,
        # a fresh created_ts, nothing else changed.
        db.execute(
            f"""
            INSERT INTO sqlmesh._intervals
            SELECT * REPLACE (
                'probe_pending_restatement' AS id,
                (SELECT MAX(created_ts) + 86400000 FROM sqlmesh._intervals)
                    AS created_ts,
                CAST(NULL AS TEXT) AS identifier,
                CAST(NULL AS TEXT) AS dev_version,
                TRUE AS is_pending_restatement
            )
            FROM sqlmesh._intervals
            WHERE {strip_catalog} = ? AND NOT is_dev AND NOT is_removed
            ORDER BY created_ts DESC LIMIT 1
            """,  # noqa: S608  # strip_catalog is a module-local literal, not input
            [model_name],
        )

        after = db.execute(
            "SELECT last_executed_at FROM meta.model_freshness WHERE model_name = ?",
            [model_name],
        ).fetchone()

    assert after is not None, f"{model_name} vanished from the view"
    assert after[0] == before, (
        f"a requested-but-unbackfilled restatement moved {model_name}'s "
        f"last_executed_at from {before} to {after[0]} — a restatement that "
        "fails mid-backfill would report the stale model as freshly rebuilt"
    )
