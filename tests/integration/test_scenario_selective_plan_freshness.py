# ruff: noqa: S101
"""Regression scenarios for what may and may not clear transform ``pending``.

Both need a real SQLMesh apply to reproduce. The first guards the fail-open (a
selective plan must not clear the flag); the second guards the fail-closed (a
real refresh must).

SQLMesh finalizes the environment on every promotion of ``prod``, so a plan
that rebuilds one model — ``moneybin transform restate --model`` — advances
``_environments.finalized_ts`` without touching anything else. A freshness
check keyed on that global stamp therefore reports the whole warehouse fresh
while every untouched model still holds pre-import data.

That is a fail-open in the exact signal whose job is catching staleness, and it
only reproduces against a real SQLMesh apply followed by a real selective plan,
which is why it is guarded here rather than in the unit suite.

Note the narrow trigger: a selective plan with *nothing to do* is a no-op and
never reaches the finalize stage. ``materialize_seeds()`` on unchanged seeds
leaves ``finalized_ts`` untouched, so it does not reproduce this. A restatement
always has work, which is what makes it the reproducer.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.services.import_service import ImportService
from moneybin.services.transform_service import TransformService
from moneybin.tables import FCT_SECURITY_PRICES

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "ofx"
_KEY = "scenario-selective-plan-key-0123456789"

# A FULL model that reads no OFX data, so restating it cannot legitimately
# clear staleness caused by newly landed OFX rows.
_UNRELATED_FULL_MODEL = FCT_SECURITY_PRICES.full_name


def _secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _KEY
    return store


def _build_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    db_path = tmp_path / "selective_plan_freshness.duckdb"
    db = Database(db_path, secret_store=_secret_store(), read_only=False)
    settings = MagicMock()
    settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: settings)
    return db


@pytest.mark.integration
@pytest.mark.slow
def test_restating_one_model_does_not_report_the_warehouse_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Land raw rows without refreshing, then restate an unrelated model.

    The restatement rebuilds ``core.fct_security_prices`` and nothing else, so
    every model that reads the newly landed OFX rows is still stale afterwards
    and ``pending`` must stay true. Keyed on the global promotion stamp it flips
    to false, telling a user — or an agent — that no refresh is needed.

    The expectation is derived from what the restatement touches, not from
    observed output: ``core.fct_security_prices`` reads ``raw.security_prices``
    and no ``raw.ofx_*`` table, so it cannot consume the rows just imported.
    """
    db = _build_db(tmp_path, monkeypatch)
    first = FIXTURES_DIR / "sample_minimal.ofx"
    second = FIXTURES_DIR / "multi_account_sample.ofx"
    for fixture in (first, second):
        assert fixture.exists(), f"missing fixture: {fixture}"

    # Build the warehouse for real, so "pending" starts from a clean baseline.
    built = ImportService(db).import_files([first], refresh=True)
    assert built.imported_count == 1
    assert built.transforms_applied is True
    assert TransformService(db).freshness().pending is False, (
        "baseline is wrong: the warehouse should be fresh right after a refresh"
    )

    # Land new raw rows and deliberately skip the refresh.
    landed = ImportService(db).import_files([second], refresh=False)
    assert landed.imported_count == 1
    assert landed.transforms_applied is False
    assert TransformService(db).freshness().pending is True, (
        "sanity check failed: newly landed raw rows must read as pending"
    )

    # `moneybin transform restate --model` runs exactly this plan. It promotes
    # prod without rebuilding the models that consume the rows just landed.
    with sqlmesh_context(db) as ctx:
        ctx.plan(
            restate_models=[_UNRELATED_FULL_MODEL],
            start="2020-01-01",
            end=None,
            auto_apply=True,
            no_prompts=True,
        )

    assert TransformService(db).freshness().pending is True, (
        f"fail-open: restating {_UNRELATED_FULL_MODEL} cleared `pending` while "
        "every model that reads the newly landed raw rows is still stale"
    )


@pytest.mark.integration
@pytest.mark.slow
def test_a_full_refresh_after_the_first_build_clears_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other half of the contract: a real refresh must clear the flag.

    Only the *first* apply executes every model. After that, `apply()` restates
    the FULL models and SQLMesh leaves VIEW and SEED snapshots alone — their
    interval is already complete, so `run()` finds nothing missing and records
    no new one. A staleness minimum taken over those kinds therefore freezes at
    the first build, and every later import pins `pending` true with no refresh
    able to clear it — the mirror-image fail-closed of the case above.

    The expectation is derived from what a refresh means, not from observed
    output: `import_files(refresh=True)` lands rows and then runs the pipeline
    that consumes them, so nothing is left for a second refresh to do.
    """
    db = _build_db(tmp_path, monkeypatch)
    first = FIXTURES_DIR / "sample_minimal.ofx"
    second = FIXTURES_DIR / "multi_account_sample.ofx"

    built = ImportService(db).import_files([first], refresh=True)
    assert built.transforms_applied is True
    assert TransformService(db).freshness().pending is False, (
        "baseline is wrong: the warehouse should be fresh right after a refresh"
    )

    again = ImportService(db).import_files([second], refresh=True)
    assert again.transforms_applied is True
    assert TransformService(db).freshness().pending is False, (
        "fail-closed: a completed refresh left `pending` true, so no user or "
        "agent action can ever clear the flag"
    )
