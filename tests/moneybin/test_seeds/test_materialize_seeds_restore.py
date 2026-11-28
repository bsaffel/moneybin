"""Regression: an interrupted seed materialization restores the placeholders.

``materialize_seeds`` drops the empty ``seeds.*`` placeholder tables before the
SQLMesh plan. A failure or a cancel (Ctrl-C) anywhere from the drop to the
context exit must recreate them, and the original error must stay primary even
when the restore itself fails (MB-298).
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin import seeds
from moneybin.database import Database
from moneybin.seeds import SEED_PLACEHOLDERS, materialize_seeds
from moneybin.tables import SEED_CATEGORIES

_WARNING = "Seed setup is incomplete; run 'moneybin transform seed' to finish it"


def _placeholder_types(db: Database) -> dict[str, str]:
    rows = db.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'seeds'"
    ).fetchall()
    types = dict(rows)
    return {t.name: types.get(t.name, "MISSING") for t in SEED_PLACEHOLDERS}


def _assert_placeholders_restored(db: Database) -> None:
    assert _placeholder_types(db) == {t.name: "BASE TABLE" for t in SEED_PLACEHOLDERS}


def _failing_context(error: BaseException, stage: str) -> Callable[[Database], Any]:
    """A ``sqlmesh_context`` stand-in that raises at entry, plan, or exit."""

    @contextmanager
    def fake(db: Database) -> Generator[Any, None, None]:
        if stage == "entry":
            raise error
        ctx = MagicMock()
        if stage == "plan":
            ctx.plan.side_effect = error
        yield ctx
        if stage == "exit":
            raise error

    return fake


@pytest.mark.parametrize("stage", ["entry", "plan", "exit"])
@pytest.mark.parametrize(
    "error_type", [KeyboardInterrupt, RuntimeError], ids=["cancel", "failure"]
)
def test_failure_or_cancel_restores_placeholders_once(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    error_type: type[BaseException],
) -> None:
    error = error_type("seed plan interrupted")
    monkeypatch.setattr(
        "moneybin.database.sqlmesh_context", _failing_context(error, stage)
    )
    restore_calls = MagicMock(wraps=seeds.refresh_views)
    monkeypatch.setattr(seeds, "refresh_views", restore_calls)

    with pytest.raises(error_type) as exc_info:
        materialize_seeds(db)

    assert exc_info.value is error
    assert restore_calls.call_count == 1
    _assert_placeholders_restored(db)


def test_cancel_during_placeholder_removal_restores_placeholders(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    def drop_one_then_cancel(target: Database) -> None:
        target.execute(f"DROP TABLE {SEED_CATEGORIES.full_name}")  # TableRef constant
        raise KeyboardInterrupt

    monkeypatch.setattr(seeds, "_drop_seed_placeholders", drop_one_then_cancel)
    sqlmesh = MagicMock()
    monkeypatch.setattr("moneybin.database.sqlmesh_context", sqlmesh)

    with pytest.raises(KeyboardInterrupt):
        materialize_seeds(db)

    sqlmesh.assert_not_called()
    _assert_placeholders_restored(db)


def test_restore_failure_keeps_original_error_and_logs_fixed_warning(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = RuntimeError("seed plan failed")
    monkeypatch.setattr(
        "moneybin.database.sqlmesh_context", _failing_context(error, "plan")
    )
    restore_error = ValueError("restore failed: SELECT * FROM seeds.categories")
    monkeypatch.setattr(seeds, "refresh_views", MagicMock(side_effect=restore_error))

    with caplog.at_level(logging.WARNING, logger="moneybin.seeds"):
        with pytest.raises(RuntimeError) as exc_info:
            materialize_seeds(db)

    assert exc_info.value is error
    assert exc_info.value.__context__ is None
    frames = [f.name for f in traceback.extract_tb(error.__traceback__)]
    assert "materialize_seeds" in frames
    records = [r for r in caplog.records if r.name == "moneybin.seeds"]
    warnings = [r for r in records if r.levelno >= logging.WARNING]
    assert [r.getMessage() for r in warnings] == [_WARNING]
    assert warnings[0].exc_info is None
    assert "restore failed" not in caplog.text
    assert "SELECT" not in caplog.text


@pytest.mark.parametrize(
    "restore_error", [ValueError("restore failed"), SystemExit(3)], ids=type
)
def test_cancel_stays_primary_when_restore_fails(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    restore_error: BaseException,
) -> None:
    error = KeyboardInterrupt()
    monkeypatch.setattr(
        "moneybin.database.sqlmesh_context", _failing_context(error, "plan")
    )
    monkeypatch.setattr(seeds, "refresh_views", MagicMock(side_effect=restore_error))

    with pytest.raises(KeyboardInterrupt) as exc_info:
        materialize_seeds(db)

    assert exc_info.value is error


def test_cancel_during_restore_outranks_ordinary_failure(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = RuntimeError("seed plan failed")
    monkeypatch.setattr(
        "moneybin.database.sqlmesh_context", _failing_context(error, "plan")
    )
    cancel = KeyboardInterrupt()
    monkeypatch.setattr(seeds, "refresh_views", MagicMock(side_effect=cancel))

    with caplog.at_level(logging.WARNING, logger="moneybin.seeds"):
        with pytest.raises(KeyboardInterrupt) as exc_info:
            materialize_seeds(db)

    assert exc_info.value is cancel
    assert exc_info.value.__context__ is error
    assert [r.getMessage() for r in caplog.records if r.name == "moneybin.seeds"][
        -1:
    ] == [_WARNING]
