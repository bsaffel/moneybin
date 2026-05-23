"""Tests for @mcp_tool(dynamic_classification=True) mode.

Verifies that the decorator preserves per-call sensitivity and classes_returned
from the envelope, does not re-redact, and logs the per-call values.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@mcp_tool(dynamic_classification=True)
def _dyn_tool() -> ResponseEnvelope[Any]:
    return build_envelope(
        data=[{"account_id": "****1234", "amount": -5}],
        sensitivity="high",
        classes_returned=["account_identifier", "txn_amount"],
    )


@pytest.mark.unit
def test_dynamic_tool_preserves_per_call_sensitivity() -> None:
    """Decorator must NOT stamp the per-call sensitivity down/up to the static placeholder."""
    env = asyncio.run(_dyn_tool())  # type: ignore[arg-type]
    assert env.summary.sensitivity == "high"


@pytest.mark.unit
def test_dynamic_tool_does_not_re_redact() -> None:
    """Decorator must NOT re-apply redact_typed — value already masked by the tool."""
    env = asyncio.run(_dyn_tool())  # type: ignore[arg-type]
    assert env.data == [{"account_id": "****1234", "amount": -5}]


@pytest.mark.unit
async def test_dynamic_tool_logs_per_call_classes() -> None:
    """Privacy log event must contain the envelope's per-call classes_returned."""
    captured: list[Any] = []

    def _capture_event(event: Any) -> None:
        captured.append(event)

    with patch("moneybin.mcp.decorator.write_privacy_event", _capture_event):
        await _dyn_tool()  # type: ignore[arg-type]

    assert len(captured) == 1
    event = captured[0]
    assert event["classes_returned"] == ["account_identifier", "txn_amount"]
    assert event["sensitivity"] == "high"


@pytest.mark.unit
async def test_dynamic_tool_critical_sensitivity_preserved() -> None:
    """A dynamic tool returning critical sensitivity is not stamped to high."""

    @mcp_tool(dynamic_classification=True)
    def _critical_tool() -> ResponseEnvelope[Any]:
        return build_envelope(
            data=[{"account_id": "****5678"}],
            sensitivity="critical",
            classes_returned=["account_identifier"],
        )

    env = await _critical_tool()  # type: ignore[arg-type]
    assert env.summary.sensitivity == "critical"


@pytest.mark.unit
async def test_dynamic_tool_error_envelope_logs_unclassified() -> None:
    """Error envelopes from dynamic tools (no classes_returned) log as unclassified."""
    from moneybin.errors import UserError
    from moneybin.protocol.envelope import build_error_envelope

    captured: list[Any] = []

    def _capture_event(event: Any) -> None:
        captured.append(event)

    @mcp_tool(dynamic_classification=True)
    def _error_tool() -> ResponseEnvelope[Any]:
        return build_error_envelope(
            error=UserError("bad query", code="invalid_query"),
            sensitivity="low",
        )

    with patch("moneybin.mcp.decorator.write_privacy_event", _capture_event):
        await _error_tool()  # type: ignore[arg-type]

    assert len(captured) == 1
    event = captured[0]
    # Error envelopes have no classes_returned → logged as ["unclassified"]
    assert event["classes_returned"] == ["unclassified"]
