"""In-process pipeline step callables. Each takes (setup, db, env) -> None."""

from __future__ import annotations

import logging
import os
import subprocess  # noqa: S404 — explicit command list, never shell=True
from collections.abc import Callable

from moneybin.database import Database, sqlmesh_context
from moneybin.services.categorization_service import CategorizationService
from moneybin.services.matching_service import MatchingService
from moneybin.testing.scenarios.loader import SetupSpec
from moneybin.testing.synthetic.engine import GeneratorEngine
from moneybin.testing.synthetic.writer import SyntheticWriter

logger = logging.getLogger(__name__)

StepCallable = Callable[[SetupSpec, Database, dict[str, str]], None]

# Subprocess timeout for the transform CLI escape hatch (seconds).
_TRANSFORM_SUBPROCESS_TIMEOUT_SEC = 300


def _step_generate(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    result = GeneratorEngine(
        setup.persona, seed=setup.seed, years=setup.years
    ).generate()
    SyntheticWriter(db).write(result)


def _step_load_fixtures(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    # Lazy import — fixture_loader is shipped in Task 12.
    from moneybin.testing.scenarios.fixture_loader import (  # type: ignore[import-not-found]
        load_fixture_into_db,
    )

    for spec in setup.fixtures:
        load_fixture_into_db(db, spec)


def _step_transform(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    with sqlmesh_context() as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)


def _step_match(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    MatchingService(db).run()


def _step_categorize(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    svc = CategorizationService(db)
    svc.apply_rules()
    # Bulk-categorize anything still uncategorized using merchant resolution.
    items = db.execute(
        """
        SELECT transaction_id, description
        FROM core.fct_transactions
        WHERE category IS NULL
        """
    ).fetchall()
    if items:
        svc.bulk_categorize([
            {"transaction_id": tid, "description": d} for tid, d in items
        ])


def _step_migrate(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    from moneybin.migrations import MigrationRunner

    MigrationRunner(db).apply_all()


def _step_transform_via_subprocess(
    setup: SetupSpec, db: Database, env: dict[str, str]
) -> None:
    proc = subprocess.run(
        ["uv", "run", "moneybin", "data", "transform", "apply"],  # noqa: S603, S607  # explicit command list; uv resolved via PATH
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=_TRANSFORM_SUBPROCESS_TIMEOUT_SEC,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"transform subprocess failed (rc={proc.returncode}): {proc.stderr[-500:]}"
        )


STEP_REGISTRY: dict[str, StepCallable] = {
    "generate": _step_generate,
    "load_fixtures": _step_load_fixtures,
    "transform": _step_transform,
    "match": _step_match,
    "categorize": _step_categorize,
    "migrate": _step_migrate,
    "transform_via_subprocess": _step_transform_via_subprocess,
}


def run_step(name: str, setup: SetupSpec, db: Database, *, env: dict[str, str]) -> None:
    """Dispatch ``name`` through ``STEP_REGISTRY``; raise ``KeyError`` if unknown."""
    if name not in STEP_REGISTRY:
        raise KeyError(f"unknown step: {name!r}")
    logger.info(f"scenario_step.start name={name}")
    STEP_REGISTRY[name](setup, db, env)
    logger.info(f"scenario_step.done name={name}")
