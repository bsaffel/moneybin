"""Timeout guard tests for the @mcp_tool decorator."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta


def _ok_envelope() -> ResponseEnvelope:
    return ResponseEnvelope(
        summary=SummaryMeta(total_count=0, returned_count=0),
        data=[],
    )


@pytest.mark.unit
def test_sync_tool_under_cap_passes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)

    @mcp_tool(sensitivity="low")
    def fast_tool() -> ResponseEnvelope:
        return _ok_envelope()

    result = asyncio.run(fast_tool())
    assert isinstance(result, ResponseEnvelope)
    assert result.error is None


@pytest.mark.unit
def test_sync_tool_over_cap_returns_timeout_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.05)
    reset_mock = MagicMock()
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", reset_mock
    )

    @mcp_tool(sensitivity="low")
    def slow_tool() -> ResponseEnvelope:
        time.sleep(0.5)
        return _ok_envelope()

    async def _run() -> tuple[ResponseEnvelope, float]:
        started = time.monotonic()
        r = await slow_tool()
        return r, time.monotonic() - started

    result, elapsed = asyncio.run(_run())

    assert elapsed < 0.4, "timeout did not fire within reasonable bound"
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "timed_out"
    assert result.error.details is not None
    assert result.error.details["tool"] == "slow_tool"
    assert result.error.details["timeout_s"] == 0.05
    assert result.error.details["elapsed_s"] >= 0.05
    reset_mock.assert_called_once()


@pytest.mark.unit
def test_async_tool_over_cap_returns_timeout_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.05)
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", MagicMock()
    )

    @mcp_tool(sensitivity="low")
    async def slow_tool() -> ResponseEnvelope:
        await asyncio.sleep(0.5)
        return _ok_envelope()

    result = asyncio.run(slow_tool())
    assert result.error is not None
    assert result.error.code == "timed_out"


@pytest.mark.unit
def test_timeout_logs_low_cardinality_line(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.02)
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", MagicMock()
    )

    @mcp_tool(sensitivity="low")
    async def slow_tool(account_number: str = "secret-123") -> ResponseEnvelope:
        await asyncio.sleep(0.5)
        return _ok_envelope()

    with caplog.at_level("WARNING"):
        asyncio.run(slow_tool(account_number="acct-redacted-do-not-log"))

    relevant = [r for r in caplog.records if "timed out" in r.getMessage().lower()]
    assert len(relevant) == 1
    assert "slow_tool" in relevant[0].getMessage()
    assert "acct-redacted-do-not-log" not in relevant[0].getMessage()


@pytest.mark.unit
def test_classified_user_error_still_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)
    from moneybin.errors import UserError

    @mcp_tool(sensitivity="low")
    def bad_tool() -> ResponseEnvelope:
        raise UserError("nope", code="not_found")

    result = asyncio.run(bad_tool())
    assert result.error is not None
    assert result.error.code == "not_found"


@pytest.mark.unit
def test_async_generator_tool_rejected_at_decoration() -> None:
    with pytest.raises(TypeError, match="async generator"):

        @mcp_tool(sensitivity="low")
        async def gen_tool() -> ResponseEnvelope:  # type: ignore[misc]
            yield  # type: ignore[misc]
