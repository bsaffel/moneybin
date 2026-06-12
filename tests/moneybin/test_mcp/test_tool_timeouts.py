"""Timeout guard tests for the @mcp_tool decorator."""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin import error_codes
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta


def _ok_envelope() -> ResponseEnvelope[Any]:
    return ResponseEnvelope(
        summary=SummaryMeta(total_count=0, returned_count=0),
        data=[],
    )


@pytest.mark.unit
async def test_sync_tool_under_cap_passes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)

    @mcp_tool(dynamic_classification=True)
    def fast_tool() -> ResponseEnvelope[Any]:
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

    @mcp_tool(dynamic_classification=True)
    def slow_tool() -> ResponseEnvelope[Any]:
        time.sleep(0.5)
        return _ok_envelope()

    async def _run() -> tuple[ResponseEnvelope[Any], float]:
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
    assert result.error.code == error_codes.INFRA_TIMED_OUT
    assert result.error.details is not None
    assert result.error.details["tool"] == "slow_tool"
    assert result.error.details["timeout_s"] == 0.05
    assert result.error.details["elapsed_s"] >= 0.05
    # slow_tool never opened a DB connection, so the timeout cleanup has nothing
    # of its own to reset — and it must NOT fall back to the global active writer
    # (which would collaterally interrupt a different call's healthy connection).
    reset_mock.assert_not_called()


@pytest.mark.unit
async def test_timed_out_tool_resets_only_its_own_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timed-out tool that acquired a write conn resets THAT conn, not the global slot."""
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.05)
    reset_mock = MagicMock()
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", reset_mock
    )
    sentinel = object()  # stands in for this call's acquired Database

    @mcp_tool(dynamic_classification=True)
    def slow_tool() -> ResponseEnvelope[Any]:
        from moneybin.database import (
            _write_conn_thread_local,  # type: ignore[reportPrivateUsage]  # test-only: simulate get_database registering its conn
        )

        # The decorator points conn_holder at this call's per-call list before
        # running the sync body; mirror get_database registering its connection.
        holder = getattr(_write_conn_thread_local, "conn_holder", None)
        assert holder is not None
        holder[0] = sentinel
        time.sleep(0.5)
        return _ok_envelope()

    await slow_tool()
    reset_mock.assert_called_once_with(sentinel)


@pytest.mark.unit
async def test_async_tool_over_cap_returns_timeout_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.05)
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", MagicMock()
    )

    @mcp_tool(dynamic_classification=True)
    async def slow_tool() -> ResponseEnvelope[Any]:
        await asyncio.sleep(0.5)
        return _ok_envelope()

    result = await slow_tool()
    assert result.error is not None
    assert result.error.code == error_codes.INFRA_TIMED_OUT


@pytest.mark.unit
async def test_timeout_logs_low_cardinality_line(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.02)
    monkeypatch.setattr(
        "moneybin.mcp.decorator.interrupt_and_reset_database", MagicMock()
    )

    @mcp_tool(dynamic_classification=True)
    async def slow_tool(account_number: str = "secret-123") -> ResponseEnvelope[Any]:
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

    @mcp_tool(dynamic_classification=True)
    def bad_tool() -> ResponseEnvelope[Any]:
        raise UserError("nope", code=error_codes.MUTATION_NOT_FOUND)

    result = await bad_tool()
    assert result.error is not None
    assert result.error.code == error_codes.MUTATION_NOT_FOUND


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

    @mcp_tool(dynamic_classification=True)
    def inner_timeout_tool() -> ResponseEnvelope[Any]:
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

        @mcp_tool(dynamic_classification=True)
        async def gen_tool() -> ResponseEnvelope[Any]:  # type: ignore[misc]
            yield  # type: ignore[misc]


@pytest.mark.unit
def test_sync_generator_tool_rejected_at_decoration() -> None:
    with pytest.raises(TypeError, match="sync generator"):

        @mcp_tool(dynamic_classification=True)
        def gen_tool() -> ResponseEnvelope[Any]:  # type: ignore[misc]
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
    Database(
        db_path, secret_store=mock_secret_store, no_auto_upgrade=True, read_only=False
    ).close()

    from contextlib import contextmanager

    @contextmanager
    def _fake_get_database(read_only: bool = False, **_: object):  # type: ignore[misc]
        conn = Database(
            db_path,
            secret_store=mock_secret_store,
            no_auto_upgrade=True,
            read_only=False,
        )
        if not read_only:
            with db_module._active_write_lock:  # pyright: ignore[reportPrivateUsage]
                db_module._active_write_conn = conn  # pyright: ignore[reportPrivateUsage]
            # Mirror real get_database: register the per-call holder so the
            # decorator's timeout cleanup resets THIS connection (rather than
            # finding [0]=None and skipping, which the guarded cleanup now does).
            holder = getattr(
                db_module._write_conn_thread_local,  # pyright: ignore[reportPrivateUsage]
                "conn_holder",
                None,
            )
            if holder is not None:
                holder[0] = conn
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
    # Cap must exceed the fake's real-Database open time. The decorator's
    # timeout-reset guard reads the per-call holder, which get_database (and this
    # fake) registers only AFTER the connection opens. Too short a cap lets the
    # timeout fire mid-open — before the holder is set — so the reset is skipped
    # and hang_tool never releases. Production caps (30 s) dwarf the open, so
    # this only bites the test; 1 s gives ample headroom under loaded CI.
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 1.0)

    @mcp_tool(dynamic_classification=True)
    def hang_tool() -> ResponseEnvelope[Any]:
        with _fake_get_database() as _db:
            _stop.wait(timeout=10.0)  # blocks until interrupt_and_reset fires
        _conn_released.set()  # connection is now closed by this (owning) thread
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0), data=[]
        )

    @mcp_tool(dynamic_classification=True)
    def quick_tool() -> ResponseEnvelope[Any]:
        # Wait until hang_tool's background thread has closed its connection
        # before opening a new one to the same file. Assert the wait so a
        # missed release is reported as such, not as a downstream lock error.
        assert _conn_released.wait(timeout=5.0), (
            "hang_tool never released its connection"
        )
        with _fake_get_database() as db:
            rows = db.execute("SELECT 42 AS x").fetchall()
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=len(rows), returned_count=len(rows)),
            data=[{"x": rows[0][0]}],
        )

    first = await hang_tool()
    assert first.error is not None and first.error.code == error_codes.INFRA_TIMED_OUT

    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)
    second = await quick_tool()
    assert second.error is None
    assert second.data == [{"x": 42}]
