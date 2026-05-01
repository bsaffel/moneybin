"""Scenario orchestrator.

Boots a fresh encrypted Database in a tempdir, dispatches the YAML pipeline
through the step registry, runs assertions/expectations/evaluations, and
returns a ``ScenarioResult`` describing the outcome.
"""

from __future__ import annotations

import importlib
import logging
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

from moneybin.database import Database, close_database, get_database
from moneybin.validation.assertions import assert_sqlmesh_catalog_matches
from moneybin.validation.result import AssertionResult, EvaluationResult
from tests.scenarios._runner.expectations import (
    ExpectationResult,
    verify_expectations,
)
from tests.scenarios._runner.loader import (
    AssertionSpec,
    EvaluationSpec,
    Scenario,
)
from tests.scenarios._runner.result import ScenarioResult
from tests.scenarios._runner.steps import run_step

logger = logging.getLogger(__name__)


# Assertion functions whose first argument is a ``Database`` rather than a
# DuckDB connection. Everything else takes a connection.
_DATABASE_ASSERTION_FNS = frozenset({
    "assert_sqlmesh_catalog_matches",
    "assert_migrations_at_head",
    "assert_min_rows",
    "assert_no_unencrypted_db_files",
})


@contextmanager
def _patched_env(env: dict[str, str]):
    """Temporarily set env vars, restoring originals on exit."""
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@contextmanager
def _restored_profile():
    """Save the caller's current profile and restore it on exit.

    The runner mutates the global profile via ``set_current_profile("scenario")``;
    without this context manager a long-lived process (tests, programmatic use)
    would have its profile silently switched and any cached settings invalidated.
    """
    from moneybin import config as _config
    from moneybin.config import clear_settings_cache, set_current_profile

    saved = _config._current_profile  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001 — internal save/restore
    try:
        yield
    finally:
        clear_settings_cache()
        if saved is not None:
            set_current_profile(saved)
        else:
            _config._current_profile = None  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001 — internal save/restore


def run_scenario(scenario: Scenario, *, keep_tmpdir: bool = False) -> ScenarioResult:
    """Run ``scenario`` end-to-end and return a ``ScenarioResult``.

    The runner provisions a profile under a fresh tempdir, opens an
    encrypted ``Database``, dispatches the configured pipeline steps, and
    then evaluates assertions, expectations, and evaluations against the
    resulting state.

    Args:
        scenario: A validated scenario specification.
        keep_tmpdir: If True, leave the tempdir in place after the run for
            post-mortem inspection. Otherwise the tempdir is removed.

    Returns:
        A ``ScenarioResult`` holding the scenario name, pass/fail boolean,
        halted reason (if the run was aborted early), and per-result lists
        for assertions, expectations, and evaluations.
    """
    started = time.perf_counter()
    tmp = tempfile.mkdtemp(prefix=f"scenario-{scenario.name}-")
    env = {"MONEYBIN_HOME": tmp, "MONEYBIN_PROFILE": "scenario"}
    # Propagate encryption key to subprocess steps (e.g., transform_via_subprocess).
    if encryption_key := os.environ.get("MONEYBIN_DATABASE__ENCRYPTION_KEY"):
        env["MONEYBIN_DATABASE__ENCRYPTION_KEY"] = encryption_key
    # Propagate keyring backend so subprocess uses MemoryKeyring instead of system keyring.
    if keyring_backend := os.environ.get("PYTHON_KEYRING_BACKEND"):
        env["PYTHON_KEYRING_BACKEND"] = keyring_backend
        # Also propagate PYTHONPATH so subprocess can import MemoryKeyring.
        if pythonpath := os.environ.get("PYTHONPATH"):
            env["PYTHONPATH"] = pythonpath
    cleanup = not keep_tmpdir

    db: Database | None = None
    try:
        with _patched_env(env), _restored_profile():
            db = _bootstrap_database()

            preflight = assert_sqlmesh_catalog_matches(db)
            if not preflight.passed:
                return _build_result(
                    scenario=scenario,
                    started=started,
                    tmpdir=tmp,
                    keep_tmpdir=keep_tmpdir,
                    assertions=[preflight],
                    expectations=[],
                    evaluations=[],
                    halted="catalog wiring failed pre-flight",
                )

            try:
                for step in scenario.pipeline:
                    run_step(step, scenario.setup, db, env=env)
                    # Steps may close the singleton (e.g., to release the
                    # DuckDB file lock for a subprocess). Re-fetch so the
                    # next step / assertion phase has a live connection.
                    db = get_database()
            except Exception as exc:  # noqa: BLE001 — surface as halted result
                # Don't use logger.exception — tracebacks may include local
                # variables holding amounts/descriptions (PII rule).
                logger.error(
                    f"scenario {scenario.name} pipeline crashed: {type(exc).__name__}"
                )
                logger.debug("scenario pipeline traceback", exc_info=True)
                return _build_result(
                    scenario=scenario,
                    started=started,
                    tmpdir=tmp,
                    keep_tmpdir=keep_tmpdir,
                    assertions=[preflight],
                    expectations=[],
                    evaluations=[],
                    # Use type name only — full str(exc) may carry amounts /
                    # descriptions from local variables (PII rule).
                    halted=f"pipeline step crashed: {type(exc).__name__}",
                )

            assertions = [
                _run_assertion(a, db, tmpdir=tmp) for a in scenario.assertions
            ]
            try:
                expectations = verify_expectations(db, scenario.expectations)
            except Exception as exc:  # noqa: BLE001 — surface as halted result
                logger.error(
                    f"scenario {scenario.name} expectations crashed: "
                    f"{type(exc).__name__}"
                )
                logger.debug("scenario expectations traceback", exc_info=True)
                return _build_result(
                    scenario=scenario,
                    started=started,
                    tmpdir=tmp,
                    keep_tmpdir=keep_tmpdir,
                    assertions=[preflight, *assertions],
                    expectations=[],
                    evaluations=[],
                    halted=f"expectations crashed: {type(exc).__name__}",
                )
            evaluations = [_run_evaluation(e, db) for e in scenario.evaluations]

            return _build_result(
                scenario=scenario,
                started=started,
                tmpdir=tmp,
                keep_tmpdir=keep_tmpdir,
                assertions=[preflight, *assertions],
                expectations=expectations,
                evaluations=evaluations,
            )
    finally:
        # Close the singleton Database before tempdir removal so DuckDB's
        # file handles are released cleanly on every platform.
        if db is not None:
            close_database()
        if cleanup:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            logger.info(f"scenario.tmpdir_kept path={tmp}")


def _bootstrap_database() -> Database:
    """Create the scenario profile and return the singleton ``Database``.

    Assumes ``MONEYBIN_HOME`` and ``MONEYBIN_PROFILE`` are already set in
    the environment. Invalidates any previously cached settings so the new
    profile's encrypted DB path is used.
    """
    from moneybin.config import clear_settings_cache, set_current_profile
    from moneybin.services.profile_service import ProfileService

    # Drop any pre-existing module singleton so we don't accidentally reuse
    # a Database opened against the caller's profile/path. Without this, a
    # long-lived process that already called ``get_database()`` would have
    # the scenario run silently against the caller's data.
    close_database()

    # Reset any previously cached settings/profile so subsequent calls pick
    # up the patched env vars.
    clear_settings_cache()
    set_current_profile("scenario")

    ProfileService().create("scenario")

    # Re-clear so the profile-create's side effects (which may have populated
    # the cache via init_db helpers) don't survive into the runner's own
    # Database singleton.
    clear_settings_cache()
    set_current_profile("scenario")

    return get_database()


def _resolve_runtime_args(args: dict[str, Any], *, tmpdir: str) -> dict[str, Any]:
    """Substitute well-known runtime sentinels in YAML-supplied assertion args."""
    return {k: (Path(tmpdir) if v == "from_runtime" else v) for k, v in args.items()}


def _run_assertion(
    spec: AssertionSpec, db: Database, *, tmpdir: str
) -> AssertionResult:
    args = _resolve_runtime_args(spec.args, tmpdir=tmpdir)
    try:
        fn = _resolve_assertion(spec.fn)
        result = (
            fn(db, **args)
            if spec.fn in _DATABASE_ASSERTION_FNS
            else fn(db.conn, **args)
        )
    except Exception as exc:  # noqa: BLE001 — surface as structured failure
        logger.error(f"assertion {spec.name} crashed: {type(exc).__name__}")
        logger.debug("assertion traceback", exc_info=True)
        return AssertionResult(
            name=spec.name,
            passed=False,
            details={"args": args},
            error=str(exc),
        )
    # Preserve the scenario-author's name so result output matches the YAML.
    return AssertionResult(
        name=spec.name,
        passed=result.passed,
        details=result.details,
        error=result.error,
    )


def _run_evaluation(spec: EvaluationSpec, db: Database) -> EvaluationResult:
    try:
        fn = _resolve_evaluation(spec.fn)
        return fn(db, threshold=spec.threshold.min, **spec.args)
    except Exception as exc:  # noqa: BLE001 — surface as structured failure
        logger.error(f"evaluation {spec.name} crashed: {type(exc).__name__}")
        logger.debug("evaluation traceback", exc_info=True)
        return EvaluationResult(
            name=spec.name,
            metric=spec.threshold.metric,
            value=0.0,
            threshold=spec.threshold.min,
            passed=False,
            breakdown={"error": str(exc)},
        )


def _resolve_assertion(fn_name: str):
    mod = importlib.import_module("moneybin.validation.assertions")
    if not hasattr(mod, fn_name):
        raise ValueError(f"unknown assertion fn: {fn_name}")
    return getattr(mod, fn_name)


def _resolve_evaluation(fn_name: str):
    mod = importlib.import_module("moneybin.validation.evaluations")
    if not hasattr(mod, fn_name):
        raise ValueError(f"unknown evaluation fn: {fn_name}")
    return getattr(mod, fn_name)


def _build_result(
    *,
    scenario: Scenario,
    started: float,
    tmpdir: str,
    keep_tmpdir: bool = False,
    assertions: list[AssertionResult],
    expectations: list[ExpectationResult],
    evaluations: list[EvaluationResult],
    halted: str | None = None,
) -> ScenarioResult:
    duration = round(time.perf_counter() - started, 2)
    all_a = all(a.passed for a in assertions)
    all_e = all(e.passed for e in expectations) if expectations else True
    all_v = all(e.passed for e in evaluations) if evaluations else True
    passed = all_a and all_e and all_v and halted is None

    return ScenarioResult(
        scenario=scenario.name,
        passed=passed,
        duration_seconds=duration,
        tmpdir=tmpdir if keep_tmpdir else None,
        halted=halted,
        assertions=[asdict(a) for a in assertions],
        expectations=[asdict(e) for e in expectations],
        evaluations=[asdict(e) for e in evaluations],
    )
