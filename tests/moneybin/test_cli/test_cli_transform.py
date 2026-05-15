"""Tests for transform CLI commands (text output paths and restate).

JSON-output parity for status/plan/apply/validate/audit lives in
``test_transform_json_output.py``; this file covers the text rendering and
the restate command (which still drives ``sqlmesh_context`` directly).
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transform import app
from moneybin.services.transform_service import (
    AuditResult,
    TransformStatus,
    ValidationResult,
)

runner = CliRunner()


@contextmanager
def _fake_get_database(*_a: Any, **_kw: Any) -> Generator[MagicMock, None, None]:
    """Stub get_database so the CLI never opens a real DB."""
    yield MagicMock()


def _mock_sqlmesh_context() -> tuple[Any, MagicMock]:
    """Create a mock sqlmesh_context that yields a MagicMock context."""
    mock_ctx = MagicMock()
    mock_ctx.state_reader.get_environment.return_value = None

    @contextmanager
    def _ctx(db: Any, **kwargs: Any) -> Generator[MagicMock, None, None]:  # noqa: ARG001 — absorb db and sqlmesh_root kwarg
        yield mock_ctx

    return _ctx, mock_ctx


class TestTransformStatus:
    """Test transform status command (text output)."""

    def test_status_uninitialized(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Missing SQLMesh env reports the bootstrap hint."""

        def fake_status(_self: Any) -> TransformStatus:
            return TransformStatus(
                environment="prod",
                initialized=False,
                last_apply_at=None,
                pending=False,
                latest_import_at=None,
            )

        monkeypatch.setattr(
            "moneybin.services.transform_service.TransformService.status",
            fake_status,
        )
        monkeypatch.setattr("moneybin.database.get_database", _fake_get_database)

        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.transform"):
            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "No SQLMesh environment initialized yet" in caplog.text

    def test_status_renders_last_apply(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Last-apply timestamp is rendered in text mode."""

        def fake_status(_self: Any) -> TransformStatus:
            return TransformStatus(
                environment="prod",
                initialized=True,
                last_apply_at=datetime(2026, 1, 15, 12, 34, 56),
                pending=False,
                latest_import_at=None,
            )

        monkeypatch.setattr(
            "moneybin.services.transform_service.TransformService.status",
            fake_status,
        )
        monkeypatch.setattr("moneybin.database.get_database", _fake_get_database)

        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.transform"):
            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "Last apply: 2026-01-15 12:34:56" in caplog.text

    def test_status_reports_never_finalized_when_null(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Initialized env without a last_apply_at reports 'never finalized'."""

        def fake_status(_self: Any) -> TransformStatus:
            return TransformStatus(
                environment="prod",
                initialized=True,
                last_apply_at=None,
                pending=False,
                latest_import_at=None,
            )

        monkeypatch.setattr(
            "moneybin.services.transform_service.TransformService.status",
            fake_status,
        )
        monkeypatch.setattr("moneybin.database.get_database", _fake_get_database)

        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.transform"):
            result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "Last apply: never finalized" in caplog.text


class TestTransformValidate:
    """Test transform validate command (text output)."""

    def test_validate_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Valid models exit 0."""

        def fake_validate(_self: Any) -> ValidationResult:
            return ValidationResult(valid=True, errors=[])

        monkeypatch.setattr(
            "moneybin.services.transform_service.TransformService.validate",
            fake_validate,
        )
        monkeypatch.setattr("moneybin.database.get_database", _fake_get_database)

        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0, result.output


class TestTransformAudit:
    """Test transform audit command (text output)."""

    def test_audit_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Audit with all-pass result exits 0."""

        def fake_audit(_self: Any, _start: str, _end: str) -> AuditResult:
            return AuditResult(passed=2, failed=0, audits=[])

        monkeypatch.setattr(
            "moneybin.services.transform_service.TransformService.audit",
            fake_audit,
        )
        monkeypatch.setattr("moneybin.database.get_database", _fake_get_database)

        result = runner.invoke(
            app, ["audit", "--start", "2026-01-01", "--end", "2026-01-31"]
        )
        assert result.exit_code == 0, result.output

    def test_audit_failure_exits_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Any failed audit exits with code 1."""

        def fake_audit(_self: Any, _start: str, _end: str) -> AuditResult:
            return AuditResult(
                passed=0,
                failed=1,
                audits=[{"name": "x", "status": "failed", "detail": "boom"}],
            )

        monkeypatch.setattr(
            "moneybin.services.transform_service.TransformService.audit",
            fake_audit,
        )
        monkeypatch.setattr("moneybin.database.get_database", _fake_get_database)

        result = runner.invoke(
            app, ["audit", "--start", "2026-01-01", "--end", "2026-01-31"]
        )
        assert result.exit_code == 1


class TestTransformRestate:
    """Test transform restate command."""

    @patch("moneybin.database.get_database")
    @patch("moneybin.cli.commands.transform.sqlmesh_context")
    def test_restate_requires_confirmation(
        self, mock_ctx_factory: MagicMock, _mock_get_db: MagicMock
    ) -> None:
        """Transform restate prompts for confirmation."""
        ctx_fn, mock_ctx = _mock_sqlmesh_context()
        mock_ctx_factory.side_effect = ctx_fn
        result = runner.invoke(
            app,
            ["restate", "--model", "core.fct_transactions", "--start", "2026-01-01"],
            input="n\n",
        )
        assert result.exit_code == 0
        mock_ctx.plan.assert_not_called()

    @patch("moneybin.database.get_database")
    @patch("moneybin.cli.commands.transform.sqlmesh_context")
    def test_restate_with_yes(
        self, mock_ctx_factory: MagicMock, _mock_get_db: MagicMock
    ) -> None:
        """Transform restate --yes skips confirmation."""
        ctx_fn, _mock_ctx = _mock_sqlmesh_context()
        mock_ctx_factory.side_effect = ctx_fn
        result = runner.invoke(
            app,
            [
                "restate",
                "--model",
                "core.fct_transactions",
                "--start",
                "2026-01-01",
                "--yes",
            ],
        )
        assert result.exit_code == 0
