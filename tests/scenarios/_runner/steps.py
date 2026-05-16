"""In-process pipeline step callables. Each takes (setup, db, env) -> None."""

from __future__ import annotations

import logging
import os
import subprocess  # noqa: S404 — explicit command list, never shell=True
from collections.abc import Callable

from moneybin.database import Database, sqlmesh_context
from moneybin.services.categorization import CategorizationService
from moneybin.services.matching_service import MatchingService
from moneybin.synthetic.engine import GeneratorEngine
from moneybin.synthetic.models import load_persona
from moneybin.synthetic.writer import SyntheticWriter
from tests.scenarios._runner.loader import IMPORT_FIXTURES_ROOT, SetupSpec
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


def _step_import_file(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    """Run real ``ImportService.import_file`` for each entry in setup.imports.

    Each entry is processed in order so scenarios can stack the same path
    twice (re-import). ``expect_failure=True`` inverts the success check
    and may require ``expect_error_substring`` to appear in the raised
    exception. ``apply_transforms`` defaults off here — scenarios group
    transforms via the explicit ``transform`` step instead.
    """
    from moneybin.services.import_service import ImportService

    service = ImportService(db)
    for spec in setup.imports:
        path = (IMPORT_FIXTURES_ROOT / spec.path).resolve()
        try:
            service.import_file(
                path,
                apply_transforms=spec.apply_transforms,
                institution=spec.institution,
                force=spec.force,
                interactive=False,
                account_name=spec.account_name,
            )
        except Exception as exc:
            if not spec.expect_failure:
                raise
            if spec.expect_error_substring and spec.expect_error_substring not in str(
                exc
            ):
                raise AssertionError(
                    f"import_file({spec.path}): expected error substring "
                    f"{spec.expect_error_substring!r} but got "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            logger.info(
                f"scenario_import.expected_failure path={spec.path} "
                f"exc={type(exc).__name__}"
            )
            continue
        if spec.expect_failure:
            raise AssertionError(
                f"import_file({spec.path}): expected failure but call succeeded"
            )


def _step_transform(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    with sqlmesh_context(db) as ctx:
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
    # categorize_pending runs rules first, then merchant-mapping fallback.
    # categorize_items is the wrong API here — it expects pre-decided
    # categories per transaction (used by the agent/UI flow), not auto-
    # classification from descriptions.
    CategorizationService(db).categorize_pending()


def _step_migrate(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    from moneybin.migrations import MigrationRunner

    MigrationRunner(db).apply_all()


def _step_transform_via_subprocess(
    setup: SetupSpec, db: Database, env: dict[str, str]
) -> None:
    # DuckDB enforces a single-writer file lock. The subprocess can't open
    # the encrypted DB while we hold a connection — close before invoking.
    # The runner re-fetches a fresh connection after each step.
    db.close()
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
        # PII-free so it's safe for the runner's error result.
        logger.debug(f"transform subprocess stderr: {proc.stderr[-500:]}")
        raise RuntimeError(f"transform subprocess failed (rc={proc.returncode})")


STEP_REGISTRY: dict[str, StepCallable] = {
    "generate": _step_generate,
    "load_fixtures": _step_load_fixtures,
    "import_file": _step_import_file,
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
