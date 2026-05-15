"""Tests for transform_* MCP tools.

Verifies envelope shape over a ``TransformService`` whose methods are
mocked to canned dataclass instances. Keeps unit tests SQLMesh-free; the
real SQLMesh round-trip is covered by ``TransformService`` tests and
integration-level scenarios.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from fastmcp import FastMCP
from pytest import MonkeyPatch

from moneybin.mcp.tools.transform import (
    register_transform_tools,
    transform_apply,
    transform_audit,
    transform_plan,
    transform_status,
    transform_validate,
)
from moneybin.services.transform_service import (
    ApplyResult,
    AuditResult,
    TransformPlan,
    TransformService,
    TransformStatus,
    ValidationResult,
)

_EXPECTED_TOOLS = {
    "transform_status",
    "transform_plan",
    "transform_validate",
    "transform_audit",
    "transform_apply",
}


@pytest.mark.unit
async def test_register_transform_tools_registers_all_five() -> None:
    """All 5 transform tools register; restate is excluded by design."""
    srv = FastMCP("test")
    register_transform_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert _EXPECTED_TOOLS <= names
    assert "transform_restate" not in names


@pytest.mark.unit
async def test_transform_status_envelope_shape(
    mcp_db: object, monkeypatch: MonkeyPatch
) -> None:
    """Envelope.data exposes environment, initialized, pending, last_apply_at, latest_import_at."""
    canned = TransformStatus(
        environment="prod",
        initialized=True,
        last_apply_at=datetime(2026, 1, 1, 12, 0, 0),
        pending=False,
        latest_import_at=None,
    )

    def fake_status(self: TransformService) -> TransformStatus:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.status",
        fake_status,
    )
    env = await transform_status()
    assert env.data["environment"] == "prod"
    assert env.data["initialized"] is True
    assert env.data["pending"] is False
    assert env.data["last_apply_at"] == "2026-01-01T12:00:00"
    assert env.data["latest_import_at"] is None


@pytest.mark.unit
async def test_transform_status_pending_appends_action(
    mcp_db: object, monkeypatch: MonkeyPatch
) -> None:
    """When pending=True, actions list includes a transform_apply hint."""
    canned = TransformStatus(
        environment="prod",
        initialized=True,
        last_apply_at=None,
        pending=True,
        latest_import_at=datetime(2026, 1, 2, 12, 0, 0),
    )

    def fake_status(self: TransformService) -> TransformStatus:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.status",
        fake_status,
    )
    env = await transform_status()
    assert env.data["pending"] is True
    assert any("transform_apply" in a for a in env.actions)


@pytest.mark.unit
async def test_transform_plan_envelope_shape(
    mcp_db: object, monkeypatch: MonkeyPatch
) -> None:
    """Envelope.data exposes plan fields with empty defaults on no-change."""
    canned = TransformPlan(
        has_changes=False,
        directly_modified=[],
        indirectly_modified=[],
        added=[],
        removed=[],
    )

    def fake_plan(self: TransformService) -> TransformPlan:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.plan",
        fake_plan,
    )
    env = await transform_plan()
    assert env.data["has_changes"] is False
    assert env.data["directly_modified"] == []
    assert env.data["indirectly_modified"] == []
    assert env.data["added"] == []
    assert env.data["removed"] == []


@pytest.mark.unit
async def test_transform_validate_envelope_shape(
    mcp_db: object, monkeypatch: MonkeyPatch
) -> None:
    """Envelope.data exposes valid + errors from ValidationResult."""
    canned = ValidationResult(valid=True, errors=[])

    def fake_validate(self: TransformService) -> ValidationResult:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.validate",
        fake_validate,
    )
    env = await transform_validate()
    assert env.data["valid"] is True
    assert env.data["errors"] == []


@pytest.mark.unit
async def test_transform_audit_envelope_shape(
    mcp_db: object, monkeypatch: MonkeyPatch
) -> None:
    """Envelope.data exposes passed/failed counts and the audits list."""
    canned = AuditResult(
        passed=3,
        failed=0,
        audits=[{"name": "test_audit_a", "status": "passed", "detail": None}],
    )

    def fake_audit(self: TransformService, start: str, end: str) -> AuditResult:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.audit",
        fake_audit,
    )
    env = await transform_audit(start="2020-01-01", end="2030-12-31")
    assert env.data["passed"] == 3
    assert env.data["failed"] == 0
    assert isinstance(env.data["audits"], list)


@pytest.mark.unit
async def test_transform_apply_envelope_shape(
    mcp_db: object, monkeypatch: MonkeyPatch
) -> None:
    """On success, envelope.data has applied + duration; no error key."""
    canned = ApplyResult(applied=True, duration_seconds=1.5, error=None)

    def fake_apply(self: TransformService) -> ApplyResult:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.apply",
        fake_apply,
    )
    env = await transform_apply()
    assert env.data["applied"] is True
    assert env.data["duration_seconds"] == 1.5
    assert "error" not in env.data


@pytest.mark.unit
async def test_transform_apply_envelope_includes_error_on_failure(
    mcp_db: object, monkeypatch: MonkeyPatch
) -> None:
    """On failure, envelope.data carries the error message under 'error'."""
    canned = ApplyResult(applied=False, duration_seconds=0.2, error="boom")

    def fake_apply(self: TransformService) -> ApplyResult:
        return canned

    monkeypatch.setattr(
        "moneybin.services.transform_service.TransformService.apply",
        fake_apply,
    )
    env = await transform_apply()
    assert env.data["applied"] is False
    assert env.data["error"] == "boom"


@pytest.mark.unit
def test_transform_apply_decoration_marks_write_tool() -> None:
    """transform_apply must be sensitivity=low and read_only=False."""
    assert transform_apply._mcp_sensitivity == "low"  # pyright: ignore[reportPrivateUsage]
    assert transform_apply._mcp_read_only is False  # pyright: ignore[reportPrivateUsage]
