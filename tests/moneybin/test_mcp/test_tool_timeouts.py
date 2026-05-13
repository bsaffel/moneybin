"""Timeout guard tests for the @mcp_tool decorator."""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from typing import Any
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
async def test_sync_tool_under_cap_passes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)

    @mcp_tool(sensitivity="low")
    def fast_tool() -> ResponseEnvelope:
        return _ok_envelope()

    result = await fast_tool()
    assert isinstance(result, ResponseEnvelope)
    assert result.error is None


@pytest.mark.unit
async def test_sync_tool_over_cap_returns_timeout_envelope(
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

    result, elapsed = await _run()

    # Cap=0.05 + 0.5s grace sleep (decorator awaits cleanup unwind before
    # returning) + scheduling slop. Ceiling stays well under the 0.5s sleep
    # the tool body would otherwise consume.
    assert elapsed < 0.9, "timeout did not fire within reasonable bound"
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "timed_out"
    assert result.error.details is not None
    assert result.error.details["tool"] == "slow_tool"
    assert result.error.details["timeout_s"] == 0.05
    assert result.error.details["elapsed_s"] >= 0.05
    reset_mock.assert_called_once()


@pytest.mark.unit
async def test_async_tool_over_cap_returns_timeout_envelope(
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

    result = await slow_tool()
    assert result.error is not None
    assert result.error.code == "timed_out"


@pytest.mark.unit
async def test_timeout_logs_low_cardinality_line(
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
        await slow_tool(account_number="acct-redacted-do-not-log")

    relevant = [r for r in caplog.records if "timed out" in r.getMessage().lower()]
    assert len(relevant) == 1
    assert "slow_tool" in relevant[0].getMessage()
    assert "acct-redacted-do-not-log" not in relevant[0].getMessage()


@pytest.mark.unit
async def test_classified_user_error_still_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)
    from moneybin.errors import UserError

    @mcp_tool(sensitivity="low")
    def bad_tool() -> ResponseEnvelope:
        raise UserError("nope", code="not_found")

    result = await bad_tool()
    assert result.error is not None
    assert result.error.code == "not_found"


@pytest.mark.unit
async def test_tool_raised_timeout_error_not_classified_as_cap_fired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A TimeoutError raised by the tool body must not be reported as a cap-fired timeout.

    Without the asyncio.timeout()/.expired() distinction, an unrelated
    TimeoutError (e.g., a downstream HTTP call) would be miscaught as the
    wall-clock cap firing, producing a misleading ``timed_out`` envelope
    AND tearing down the DuckDB connection unnecessarily.
    """
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)
    reset_mock = MagicMock()
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", reset_mock
    )

    @mcp_tool(sensitivity="low")
    def inner_timeout_tool() -> ResponseEnvelope:
        raise TimeoutError("downstream HTTP timeout")

    # TimeoutError is not a classified UserError, so the decorator re-raises
    # it (matching pre-existing behavior for unclassified exceptions). The
    # critical assertions are the side effects: no DB reset, no cap-fired log.
    with pytest.raises(TimeoutError, match="downstream HTTP timeout"):
        await inner_timeout_tool()

    reset_mock.assert_not_called()


@pytest.mark.unit
def test_async_generator_tool_rejected_at_decoration() -> None:
    with pytest.raises(TypeError, match="async generator"):

        @mcp_tool(sensitivity="low")
        async def gen_tool() -> ResponseEnvelope:  # type: ignore[misc]
            yield  # type: ignore[misc]


@pytest.mark.unit
def test_sync_generator_tool_rejected_at_decoration() -> None:
    with pytest.raises(TypeError, match="sync generator"):

        @mcp_tool(sensitivity="low")
        def gen_tool() -> ResponseEnvelope:  # type: ignore[misc]
            yield  # type: ignore[misc]


@pytest.mark.integration
async def test_back_to_back_call_after_timeout_succeeds(
    tmp_path: Path,
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A call that times out must release the DB lock so the next call works."""
    import moneybin.database as db_module
    import moneybin.mcp.decorator as dec_module
    from moneybin.database import Database

    # Create the test DB once — quick_tool will open fresh connections to it.
    db_path = tmp_path / "t.duckdb"
    Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True).close()

    from contextlib import contextmanager

    @contextmanager
    def _fake_get_database(read_only: bool = False, **_: object):  # type: ignore[misc]
        conn = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
        if not read_only:
            with db_module._active_write_lock:  # pyright: ignore[reportPrivateUsage]
                db_module._active_write_conn = conn  # pyright: ignore[reportPrivateUsage]
        try:
            yield conn
        finally:
            conn.close()
            if not read_only:
                with db_module._active_write_lock:  # pyright: ignore[reportPrivateUsage]
                    if db_module._active_write_conn is conn:  # pyright: ignore[reportPrivateUsage]
                        db_module._active_write_conn = None  # pyright: ignore[reportPrivateUsage]

    monkeypatch.setattr(db_module, "get_database", _fake_get_database)

    # Events for deterministic connection handoff.
    # DuckDB connections are not thread-safe: closing from a different thread
    # than the creator is unreliable under high scheduler load (n=6+ xdist
    # workers). Instead, we signal hang_tool's background thread to exit its
    # with-block so it closes the connection on the owning thread, then gate
    # quick_tool on that release.
    _stop = threading.Event()
    _conn_released = threading.Event()

    _real_irdb = dec_module.interrupt_and_reset_database

    def _interrupt_and_signal(conn: Any = None) -> None:
        _real_irdb(conn)  # run the real cleanup (clears _active_write_conn, etc.)
        _stop.set()  # unblock hang_tool so it closes its own connection

    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", _interrupt_and_signal
    )
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.1)

    @mcp_tool(sensitivity="low")
    def hang_tool() -> ResponseEnvelope:
        with _fake_get_database() as _db:
            _stop.wait(timeout=10.0)  # blocks until interrupt_and_reset fires
        _conn_released.set()  # connection is now closed by this (owning) thread
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0), data=[]
        )

    @mcp_tool(sensitivity="low")
    def quick_tool() -> ResponseEnvelope:
        # Wait until hang_tool's background thread has closed its connection
        # before opening a new one to the same file.
        _conn_released.wait(timeout=5.0)
        with _fake_get_database() as db:
            rows = db.execute("SELECT 42 AS x").fetchall()
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=len(rows), returned_count=len(rows)),
            data=[{"x": rows[0][0]}],
        )

    first = await hang_tool()
    assert first.error is not None and first.error.code == "timed_out"

    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)
    second = await quick_tool()
    assert second.error is None
    assert second.data == [{"x": 42}]
