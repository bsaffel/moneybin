"""Verify the transform apply pathway emits a post_transform checkpoint.

The actual ``ctx.plan(auto_apply=True)`` site lives in
``TransformService.apply()`` (called by both the CLI ``transform apply``
command and the equivalent MCP tool through the shared service layer).
The checkpoint must fire on the service path so both surfaces benefit
without a parallel wiring per surface.

This is a unit test of the service composition — sqlmesh_context and
MatchingService are mocked so the test never spins up a real SQLMesh
Context. Operation-type classification (``operation_type="transform_apply"``
on the underlying ``get_database`` call) is exercised separately by
``sqlmesh_command`` callers; here we only assert the checkpoint side.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from moneybin.services.transform_service import TransformService


@contextmanager
def _fake_sqlmesh_context(_db: object) -> Generator[MagicMock, None, None]:
    """Yield a mock SQLMesh context whose plan() is a no-op and run() succeeds."""
    ctx = MagicMock()
    ctx.run.return_value.is_failure = False
    yield ctx


def test_transform_apply_emits_post_transform_checkpoint() -> None:
    """Successful apply() must call db.checkpoint('post_transform') exactly once."""
    db = MagicMock()
    with (
        patch(
            "moneybin.services.transform_service.sqlmesh_context",
            _fake_sqlmesh_context,
        ),
        patch("moneybin.services.transform_service.MatchingService") as mock_matching,
        patch("moneybin.services.transform_service.refresh_views") as mock_refresh,
    ):
        mock_matching.return_value.seed_priority.return_value = None
        mock_refresh.return_value = None
        result = TransformService(db).apply()
    assert result.applied is True
    db.checkpoint.assert_called_once_with("post_transform")


def test_transform_apply_checkpoint_failure_still_reports_applied(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A post_transform checkpoint failure must not flip applied to False.

    The transforms have already committed by the time the checkpoint runs.
    A CHECKPOINT failure is a durability hint, not a correctness signal — it
    must be logged and swallowed, leaving applied=True so the caller doesn't
    think the apply failed and re-run it.
    """
    db = MagicMock()
    db.checkpoint.side_effect = RuntimeError("CHECKPOINT blew up")
    with (
        patch(
            "moneybin.services.transform_service.sqlmesh_context",
            _fake_sqlmesh_context,
        ),
        patch("moneybin.services.transform_service.MatchingService") as mock_matching,
        patch("moneybin.services.transform_service.refresh_views") as mock_refresh,
        caplog.at_level(logging.WARNING, logger="moneybin.services.transform_service"),
    ):
        mock_matching.return_value.seed_priority.return_value = None
        mock_refresh.return_value = None
        result = TransformService(db).apply()
    assert result.applied is True
    assert result.error is None
    db.checkpoint.assert_called_once_with("post_transform")
    assert any("checkpoint failed" in record.message for record in caplog.records), (
        "expected a warning that the post_transform checkpoint failed"
    )


def test_transform_apply_does_not_checkpoint_on_failure() -> None:
    """A ctx.plan failure must not trigger the post_transform checkpoint.

    The soft-fail path returns ApplyResult(applied=False) with no durable
    boundary to commit, so the counter stays untouched.
    """

    @contextmanager
    def failing_sqlmesh_context(_db: object) -> Generator[MagicMock, None, None]:
        ctx = MagicMock()
        ctx.plan.side_effect = RuntimeError("sqlmesh blew up")
        yield ctx

    db = MagicMock()
    with (
        patch(
            "moneybin.services.transform_service.sqlmesh_context",
            failing_sqlmesh_context,
        ),
        patch("moneybin.services.transform_service.MatchingService") as mock_matching,
        patch("moneybin.services.transform_service.refresh_views"),
    ):
        mock_matching.return_value.seed_priority.return_value = None
        result = TransformService(db).apply()
    assert result.applied is False
    assert result.error == "RuntimeError"
    db.checkpoint.assert_not_called()


def test_sqlmesh_command_passes_transform_apply_operation_type() -> None:
    """sqlmesh_command classifies its write open as 'transform_apply'.

    The operation_type kwarg flows through to write_lock so the file-lock
    metadata and metric labels distinguish transform runs from interactive
    writes.
    """
    from moneybin.cli.utils import sqlmesh_command

    captured: dict[str, Any] = {}

    @contextmanager
    def fake_get_database(
        *, read_only: bool, **kwargs: Any
    ) -> Generator[MagicMock, None, None]:
        captured["read_only"] = read_only
        captured["operation_type"] = kwargs.get("operation_type")
        yield MagicMock()

    with patch("moneybin.database.get_database", fake_get_database):
        with sqlmesh_command("test label") as _db:
            pass

    assert captured["read_only"] is False
    assert captured["operation_type"] == "transform_apply"
