"""CLI --output json parity for transform commands (mocked TransformService).

Mocks ``TransformService.<method>`` so these tests never spin up a real
SQLMesh Context. Integration coverage for the SQLMesh path lives in
``tests/integration/``.
"""

from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transform import app
from moneybin.services.transform_service import (
    ApplyResult,
    AuditResult,
    TransformPlan,
    TransformStatus,
    ValidationResult,
)

runner = CliRunner()


def _patch_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub ``get_database`` so the CLI never opens a real encrypted DB."""

    @contextmanager
    def fake_get_database(*_a: Any, **_kw: Any) -> Generator[MagicMock, None, None]:
        yield MagicMock()

    monkeypatch.setattr("moneybin.database.get_database", fake_get_database)


def test_transform_status_json(monkeypatch: pytest.MonkeyPatch) -> None:
    canned = TransformStatus(
        environment="prod",
        initialized=True,
        last_apply_at=datetime(2026, 1, 1, 12, 0, 0),
        pending=False,
        latest_import_at=None,
    )

    def fake_status(_self: Any) -> TransformStatus:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.status", fake_status
    )
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["status", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["environment"] == "prod"
    assert payload["data"]["initialized"] is True
    assert payload["data"]["pending"] is False
    assert payload["data"]["last_apply_at"] == "2026-01-01T12:00:00"
    assert payload["summary"]["sensitivity"] == "low"


def test_transform_status_json_pending_emits_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canned = TransformStatus(
        environment="prod",
        initialized=True,
        last_apply_at=None,
        pending=True,
        latest_import_at=datetime(2026, 2, 1, 9, 0, 0),
    )

    def fake_status(_self: Any) -> TransformStatus:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.status", fake_status
    )
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["status", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["pending"] is True
    assert "Run transform_apply to refresh derived tables" in payload["actions"]


def test_transform_apply_json(monkeypatch: pytest.MonkeyPatch) -> None:
    canned = ApplyResult(applied=True, duration_seconds=1.5, error=None)

    def fake_apply(_self: Any) -> ApplyResult:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.apply", fake_apply
    )
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["apply", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["applied"] is True
    assert payload["data"]["duration_seconds"] == 1.5
    # error key omitted when None
    assert "error" not in payload["data"]


def test_transform_plan_json(monkeypatch: pytest.MonkeyPatch) -> None:
    canned = TransformPlan(
        has_changes=False,
        directly_modified=[],
        indirectly_modified=[],
        added=[],
        removed=[],
    )

    def fake_plan(_self: Any) -> TransformPlan:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.plan", fake_plan
    )
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["plan", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["has_changes"] is False
    assert payload["data"]["directly_modified"] == []


def test_transform_validate_json(monkeypatch: pytest.MonkeyPatch) -> None:
    canned = ValidationResult(valid=True, errors=[])

    def fake_validate(_self: Any) -> ValidationResult:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.validate", fake_validate
    )
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["validate", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["valid"] is True
    assert payload["data"]["errors"] == []


def test_transform_audit_json(monkeypatch: pytest.MonkeyPatch) -> None:
    canned = AuditResult(passed=3, failed=0, audits=[])

    def fake_audit(_self: Any, _start: str, _end: str) -> AuditResult:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.audit", fake_audit
    )
    _patch_db(monkeypatch)
    result = runner.invoke(
        app,
        [
            "audit",
            "--start",
            "2020-01-01",
            "--end",
            "2030-12-31",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["data"]["passed"] == 3
    assert payload["data"]["failed"] == 0
