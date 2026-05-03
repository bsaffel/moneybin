"""Timeout guard tests for the @mcp_tool decorator."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
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


@pytest.mark.integration
def test_back_to_back_call_after_timeout_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A call that times out must release the DB lock so the next call works."""
    import moneybin.database as db_module
    from moneybin.database import Database

    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key-32-bytes-padding-padding-pad"

    db = Database(tmp_path / "t.duckdb", secret_store=mock_store, no_auto_upgrade=True)
    monkeypatch.setattr(db_module, "_database_instance", db)

    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.1)

    @mcp_tool(sensitivity="low")
    def hang_tool() -> ResponseEnvelope:
        # Spin without releasing GIL frequently to mimic a stuck native call.
        time.sleep(2.0)
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0), data=[]
        )

    @mcp_tool(sensitivity="low")
    def quick_tool() -> ResponseEnvelope:
        # Touch the DB to prove the singleton is healthy after reset.
        from moneybin.database import get_database

        rows = get_database().execute("SELECT 42 AS x").fetchall()
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=len(rows), returned_count=len(rows)),
            data=[{"x": rows[0][0]}],
        )

    first = asyncio.run(hang_tool())
    assert first.error is not None and first.error.code == "timed_out"

    # The timeout path cleared _database_instance and force-closed the original
    # connection, releasing its write lock.  Reopening the same DB file proves
    # the lock was actually dropped — if interrupt_and_reset() had failed to
    # release it, this Database() construction would hang or fail.
    db2 = Database(tmp_path / "t.duckdb", secret_store=mock_store, no_auto_upgrade=True)
    monkeypatch.setattr(db_module, "_database_instance", db2)

    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)
    second = asyncio.run(quick_tool())
    assert second.error is None
    assert second.data == [{"x": 42}]
