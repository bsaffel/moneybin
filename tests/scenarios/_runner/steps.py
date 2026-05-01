"""In-process pipeline step callables. Each takes (setup, db, env) -> None."""

from __future__ import annotations

import logging
import os
import subprocess  # noqa: S404 — explicit command list, never shell=True
from collections.abc import Callable

from moneybin.database import Database, sqlmesh_context
from moneybin.services.categorization_service import CategorizationService
from moneybin.services.matching_service import MatchingService
from moneybin.testing.synthetic.engine import GeneratorEngine
from moneybin.testing.synthetic.models import load_persona
from moneybin.testing.synthetic.writer import SyntheticWriter
from tests.scenarios._runner.loader import SetupSpec
from tests.scenarios._runner.seed_merchants import seed_merchants_from_persona

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
    from tests.scenarios._runner.fixture_loader import load_fixture_into_db

    for spec in setup.fixtures:
        load_fixture_into_db(db, spec)


def _step_transform(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    with sqlmesh_context() as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)


def _step_match(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    # Scenarios simulate human review: transfer matches are auto-accepted so
    # core.bridge_transfers and fct_transactions.transfer_pair_id populate
    # without an interactive review step. Real product flow leaves them pending.
    MatchingService(db).run(auto_accept_transfers=True)


def _step_seed_merchants(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    # Materializes synthetic merchant-prefix → category mappings into
    # app.merchants so the categorize step has rules to evaluate. Real
    # product flow seeds via auto-rules / user actions; scenarios bypass
    # that to keep the pipeline deterministic.
    seed_merchants_from_persona(db, load_persona(setup.persona))


def _step_categorize(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    # apply_deterministic runs rules first, then merchant-mapping fallback.
    # bulk_categorize is the wrong API here — it expects pre-decided
    # categories per transaction (used by the agent/UI flow), not auto-
    # classification from descriptions.
    CategorizationService(db).apply_deterministic()


def _step_migrate(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    from moneybin.migrations import MigrationRunner

    MigrationRunner(db).apply_all()


def _step_transform_via_subprocess(
    setup: SetupSpec, db: Database, env: dict[str, str]
) -> None:
    # DuckDB enforces a single-writer file lock. The subprocess can't open
    # the encrypted DB while we hold a connection — close the singleton
    # before invoking. The runner re-fetches the singleton after each step
    # so the assertion phase opens a fresh connection.
    from moneybin.database import close_database

    close_database()
    proc = subprocess.run(
        ["uv", "run", "moneybin", "transform", "apply"],  # noqa: S603, S607  # explicit command list; uv resolved via PATH
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=_TRANSFORM_SUBPROCESS_TIMEOUT_SEC,
        check=False,
    )
    if proc.returncode != 0:
        # Log stderr separately at DEBUG — it may carry DuckDB error text that
        # echoes amounts/descriptions (PII rule). The exception message stays
        # PII-free so it's safe for the runner's error envelope.
        logger.debug(f"transform subprocess stderr: {proc.stderr[-500:]}")
        raise RuntimeError(f"transform subprocess failed (rc={proc.returncode})")


STEP_REGISTRY: dict[str, StepCallable] = {
    "generate": _step_generate,
    "load_fixtures": _step_load_fixtures,
    "transform": _step_transform,
    "match": _step_match,
    "seed_merchants": _step_seed_merchants,
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
