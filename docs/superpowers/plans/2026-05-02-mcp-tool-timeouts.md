# MCP Tool Timeouts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap every MCP tool dispatch in a hard wall-clock timeout that returns a structured error envelope and releases any held DuckDB write lock so the next call succeeds.

**Architecture:** A new timeout guard inside the `@mcp_tool` decorator runs every tool under `asyncio.wait_for(..., timeout=settings.mcp.tool_timeout_seconds)`. Sync tool bodies are dispatched to a worker thread via `asyncio.to_thread`. On timeout, a new `Database.interrupt_and_reset()` helper calls `connection.interrupt()`, closes the connection, and clears the singleton so the next call reopens cleanly. The error envelope grows a structured `kind="timed_out"` shape with `tool`, `elapsed_s`, `timeout_s`. Default cap: 30s, configurable via `MoneyBinSettings.mcp.tool_timeout_seconds`.

**Tech Stack:** Python 3.12+, FastMCP, DuckDB Python API, `asyncio` (stdlib), Pydantic Settings, pytest.

---

## File Structure

**Create:**
- `tests/moneybin/test_mcp/test_tool_timeouts.py` — unit + integration coverage for the timeout path.

**Modify:**
- `src/moneybin/config.py` — add `tool_timeout_seconds` field to `MCPConfig`.
- `src/moneybin/errors.py` — add `TimeoutError` UserError + classification.
- `src/moneybin/protocol/envelope.py` — extend error payload to support `{kind, tool, elapsed_s, timeout_s}` for timeouts.
- `src/moneybin/database.py` — add `interrupt_and_reset()` method on `Database` and `interrupt_and_reset_database()` module-level helper.
- `src/moneybin/mcp/decorator.py` — wrap every decorated tool in an async timeout guard; on timeout, call `interrupt_and_reset_database()` and return a structured timeout envelope.
- `tests/moneybin/test_mcp/test_decorator.py` — extend with envelope-shape regression for the new error fields (no behavior break).

---

## Task 1: Add `tool_timeout_seconds` to MCPConfig

**Files:**
- Modify: `src/moneybin/config.py:228-255` (extend `MCPConfig`)
- Test: `tests/moneybin/test_config.py` (locate the existing MCPConfig tests; add one)

- [ ] **Step 1: Locate existing MCPConfig tests**

Run: `grep -rn "MCPConfig\|tool_timeout" tests/moneybin/test_config.py 2>/dev/null | head -20`

If `tests/moneybin/test_config.py` doesn't exist, search broader: `grep -rln "MCPConfig" tests/`. Use whatever file the existing config tests live in. If none exists, create `tests/moneybin/test_config.py` with the test below.

- [ ] **Step 2: Write failing test for the new field**

Add to the file located in Step 1:

```python
import pytest

from moneybin.config import MCPConfig


@pytest.mark.unit
def test_mcp_tool_timeout_default() -> None:
    cfg = MCPConfig()
    assert cfg.tool_timeout_seconds == 30.0


@pytest.mark.unit
def test_mcp_tool_timeout_must_be_positive() -> None:
    with pytest.raises(ValueError):
        MCPConfig(tool_timeout_seconds=0.0)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_config.py -v -k tool_timeout`
Expected: FAIL with `AttributeError: ... tool_timeout_seconds` or unexpected default.

- [ ] **Step 4: Add the field to MCPConfig**

In `src/moneybin/config.py`, inside the `MCPConfig` class (after `progressive_disclosure`), add:

```python
    tool_timeout_seconds: float = Field(
        default=30.0,
        gt=0.0,
        description=(
            "Hard wall-clock cap for any single MCP tool dispatch. On timeout, "
            "the active DuckDB statement is interrupted and the connection is "
            "reset so subsequent calls aren't wedged behind a stale write lock."
        ),
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_config.py -v -k tool_timeout`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/test_config.py
git commit -m "Add tool_timeout_seconds setting to MCPConfig"
```

---

## Task 2: Add `interrupt_and_reset` to Database

**Files:**
- Modify: `src/moneybin/database.py:475-485` (add method on `Database`); append module-level helper near line 539.
- Test: `tests/moneybin/test_database.py` (or whichever existing file holds Database tests — search if unsure).

- [ ] **Step 1: Locate existing Database tests**

Run: `grep -rln "class TestDatabase\|def test_database\|from moneybin.database" tests/moneybin/ | head`

Use the matching file. If none, create `tests/moneybin/test_database_interrupt.py`.

- [ ] **Step 2: Write failing test for `interrupt_and_reset`**

```python
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, get_database, interrupt_and_reset_database
import moneybin.database as db_module


@pytest.mark.unit
def test_interrupt_and_reset_calls_interrupt_then_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key-32-bytes-padding-padding-pad"
    db = Database(tmp_path / "t.duckdb", secret_store=mock_store, no_auto_upgrade=True)

    raw_conn = db._conn
    assert raw_conn is not None
    raw_conn.interrupt = MagicMock(wraps=raw_conn.interrupt)  # type: ignore[method-assign]

    db.interrupt_and_reset()

    raw_conn.interrupt.assert_called_once()
    assert db._conn is None
    assert db._closed is True


@pytest.mark.unit
def test_module_helper_clears_singleton(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key-32-bytes-padding-padding-pad"

    monkeypatch.setattr(db_module, "_database_instance", None)
    db = Database(tmp_path / "t.duckdb", secret_store=mock_store, no_auto_upgrade=True)
    monkeypatch.setattr(db_module, "_database_instance", db)

    interrupt_and_reset_database()

    assert db_module._database_instance is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_database_interrupt.py -v` (or the file you used)
Expected: FAIL with `AttributeError: ... interrupt_and_reset` and `ImportError: cannot import name 'interrupt_and_reset_database'`.

- [ ] **Step 4: Add method on `Database`**

In `src/moneybin/database.py`, immediately after the existing `close()` method (around line 485), add:

```python
    def interrupt_and_reset(self) -> None:
        """Interrupt any active statement and force-close the connection.

        Called from the MCP timeout path so a stuck tool releases its
        DuckDB write lock before the dispatcher returns. Best-effort:
        DuckDB's interrupt() is a no-op for some statement types (e.g.,
        mid-COPY), so we always follow with close() to guarantee the
        lock drops.
        """
        if self._conn is not None:
            try:
                self._conn.interrupt()
            except Exception:  # noqa: BLE001 — interrupt is best-effort
                pass
        self.close()
```

- [ ] **Step 5: Add module-level helper**

At the bottom of `src/moneybin/database.py` (after `close_database`), add:

```python
def interrupt_and_reset_database() -> None:
    """Interrupt and clear the singleton Database, if one exists.

    The next ``get_database()`` call will reopen a fresh connection. No-op
    if no Database has been initialized yet (e.g., timeout before any
    tool actually touched the DB).
    """
    global _database_instance  # noqa: PLW0603 — module-level singleton is intentional

    if _database_instance is not None:
        _database_instance.interrupt_and_reset()
        _database_instance = None
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_database_interrupt.py -v`
Expected: PASS (2 passed).

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/database.py tests/moneybin/test_database_interrupt.py
git commit -m "Add interrupt_and_reset for MCP timeout cleanup path"
```

---

## Task 3: Extend UserError + envelope to carry structured timeout payload

**Files:**
- Modify: `src/moneybin/errors.py` (add `details` to `UserError.to_dict`)
- Modify: `src/moneybin/protocol/envelope.py` (no shape change, just verify pass-through)
- Test: `tests/moneybin/test_protocol/test_envelope.py` if exists, else `tests/moneybin/test_mcp/test_envelope.py`.

- [ ] **Step 1: Write failing test for structured details on UserError**

Add to `tests/moneybin/test_mcp/test_envelope.py`:

```python
@pytest.mark.unit
def test_user_error_carries_structured_details() -> None:
    from moneybin.errors import UserError

    err = UserError(
        "Tool exceeded 30.0s cap",
        code="timed_out",
        hint=None,
        details={"tool": "import_inbox_sync", "elapsed_s": 30.1, "timeout_s": 30.0},
    )
    d = err.to_dict()
    assert d["code"] == "timed_out"
    assert d["details"]["tool"] == "import_inbox_sync"
    assert d["details"]["timeout_s"] == 30.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_mcp/test_envelope.py -v -k structured_details`
Expected: FAIL with `TypeError: UserError.__init__() got an unexpected keyword argument 'details'`.

- [ ] **Step 3: Extend UserError**

In `src/moneybin/errors.py`, replace the `__init__` and `to_dict` methods on `UserError` with:

```python
def __init__(
    self,
    message: str,
    *,
    code: str,
    hint: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Construct a UserError with a user-safe message, stable code, optional hint, and optional structured details."""
    super().__init__(message)
    self.message = message
    self.code = code
    self.hint = hint
    self.details = details


def to_dict(self) -> dict[str, Any]:
    """Convert to a plain dict for envelope serialization."""
    d: dict[str, Any] = {"message": self.message, "code": self.code}
    if self.hint is not None:
        d["hint"] = self.hint
    if self.details is not None:
        d["details"] = self.details
    return d
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_mcp/test_envelope.py -v -k structured_details`
Expected: PASS.

- [ ] **Step 5: Run full envelope + errors regression**

Run: `uv run pytest tests/moneybin/test_mcp/test_envelope.py tests/moneybin/test_mcp/test_decorator.py -v`
Expected: All pass — the new `details` field is opt-in, existing UserError callers unaffected.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/errors.py tests/moneybin/test_mcp/test_envelope.py
git commit -m "Add optional details payload to UserError for structured envelopes"
```

---

## Task 4: Wrap tool dispatch in async timeout guard

**Files:**
- Modify: `src/moneybin/mcp/decorator.py` (rewrite the wrapper construction to always return an async wrapper that enforces the timeout)
- Test: `tests/moneybin/test_mcp/test_tool_timeouts.py` (new)

Decision rationale: FastMCP awaits tool callables in its async dispatch loop. Wrapping the decorator output in an async function lets us use `asyncio.wait_for` uniformly. Sync tool bodies run in a thread via `asyncio.to_thread`. This is the smallest change that gives us a hard wall-clock cap on every tool with no per-tool refactor.

- [ ] **Step 1: Write failing tests for timeout behavior**

Create `tests/moneybin/test_mcp/test_tool_timeouts.py`:

```python
"""Timeout guard tests for the @mcp_tool decorator."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock, patch

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

    started = time.monotonic()
    result = asyncio.run(slow_tool())
    elapsed = time.monotonic() - started

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_mcp/test_tool_timeouts.py -v`
Expected: FAIL — current decorator returns a sync function, has no timeout, has no `_get_timeout_seconds` symbol.

- [ ] **Step 3: Rewrite the decorator with the timeout guard**

Replace the entire body of `src/moneybin/mcp/decorator.py` with:

```python
"""MCP tool decorator: sensitivity logging, timeout guard, error classification, envelope guard.

Every decorated tool is exposed as an async coroutine — FastMCP awaits it
in its dispatch loop. Sync tool bodies are dispatched to a worker thread
via ``asyncio.to_thread`` so they share the same timeout machinery.

On timeout we (a) cancel the awaited future, (b) call
``interrupt_and_reset_database()`` to drop the singleton DuckDB
connection — releasing any held write lock — and (c) return a structured
``timed_out`` error envelope. The next tool call will lazily reopen a
fresh connection.

Classified domain exceptions (``UserError``, ``DatabaseKeyError``,
``FileNotFoundError``) become error envelopes here; anything else
propagates to the server's ``mask_error_details`` boundary.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import time
from collections.abc import Callable
from typing import Any, Literal

from moneybin.database import interrupt_and_reset_database
from moneybin.errors import UserError, classify_user_error
from moneybin.mcp.privacy import Sensitivity, log_tool_call
from moneybin.protocol.envelope import ResponseEnvelope, build_error_envelope

logger = logging.getLogger(__name__)


def _get_timeout_seconds() -> float:
    """Read the configured timeout. Indirected for test monkeypatching."""
    from moneybin.config import get_settings

    return get_settings().mcp.tool_timeout_seconds


def _check_envelope(fn_name: str, result: Any) -> ResponseEnvelope:
    if not isinstance(result, ResponseEnvelope):
        msg = f"{fn_name} returned {type(result).__name__}, expected ResponseEnvelope"
        logger.error(msg)
        raise TypeError(msg)
    return result


def _classify_or_raise(fn_name: str, exc: Exception) -> ResponseEnvelope:
    """Convert a classified domain exception to an error envelope, else re-raise."""
    classified = classify_user_error(exc)
    if classified is None:
        raise exc
    logger.error(f"Tool {fn_name} raised {type(exc).__name__}: {classified.code}")
    return build_error_envelope(error=classified, sensitivity="low")


def _build_timeout_envelope(
    fn_name: str, elapsed_s: float, timeout_s: float
) -> ResponseEnvelope:
    err = UserError(
        f"Tool {fn_name} exceeded {timeout_s:.1f}s cap",
        code="timed_out",
        details={
            "tool": fn_name,
            "elapsed_s": round(elapsed_s, 3),
            "timeout_s": timeout_s,
        },
    )
    return build_error_envelope(error=err, sensitivity="low")


def mcp_tool(
    *,
    sensitivity: Literal["low", "medium", "high"],
    domain: str | None = None,
) -> Callable[..., Any]:
    """Mark a function as an MCP tool with a sensitivity tier and optional domain.

    Tools with a ``domain`` start hidden; ``moneybin_discover`` enables them
    per-session via FastMCP tag visibility. Every tool is wrapped in a
    wall-clock timeout guard — see module docstring.
    """
    tier = Sensitivity(sensitivity)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        is_coro = inspect.iscoroutinefunction(fn)

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope:
            log_tool_call(fn.__name__, tier)
            timeout_s = _get_timeout_seconds()
            started = time.monotonic()
            try:
                if is_coro:
                    coro = fn(*args, **kwargs)
                else:
                    coro = asyncio.to_thread(fn, *args, **kwargs)
                result = await asyncio.wait_for(coro, timeout=timeout_s)
            except asyncio.TimeoutError:
                elapsed = time.monotonic() - started
                logger.warning(
                    f"Tool {fn.__name__} timed out after {elapsed:.2f}s "
                    f"(cap {timeout_s:.1f}s); interrupting DB and resetting connection"
                )
                try:
                    interrupt_and_reset_database()
                except Exception as exc:  # noqa: BLE001 — cleanup must not raise
                    logger.error(
                        f"interrupt_and_reset_database failed during {fn.__name__} "
                        f"timeout cleanup: {type(exc).__name__}"
                    )
                return _build_timeout_envelope(fn.__name__, elapsed, timeout_s)
            except Exception as exc:
                return _classify_or_raise(fn.__name__, exc)
            return _check_envelope(fn.__name__, result)

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        wrapper._mcp_domain = domain  # type: ignore[attr-defined]
        return wrapper

    return decorator
```

- [ ] **Step 4: Run new tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_mcp/test_tool_timeouts.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Run existing decorator regression**

Run: `uv run pytest tests/moneybin/test_mcp/test_decorator.py -v`
Expected: Most pass. **Two existing tests will fail** because the decorator is now always async:
- `test_decorator_returns_response_envelope` — calls `my_tool()` synchronously and asserts on the dict.
- `test_decorator_raises_type_error_for_non_envelope` — same.

Update those two tests:

```python
@pytest.mark.unit
def test_decorator_returns_response_envelope(self) -> None:
    import asyncio

    @mcp_tool(sensitivity="low")
    def my_tool() -> ResponseEnvelope:
        return ResponseEnvelope(
            summary=SummaryMeta(total_count=1, returned_count=1),
            data=[{"value": 42}],
        )

    result = asyncio.run(my_tool())
    assert isinstance(result, ResponseEnvelope)
    assert result.summary.total_count == 1
    assert result.data == [{"value": 42}]


@pytest.mark.unit
def test_decorator_raises_type_error_for_non_envelope(self) -> None:
    import asyncio
    import pytest

    @mcp_tool(sensitivity="low")
    def my_tool() -> str:  # type: ignore[return]
        return "plain string result"  # type: ignore[return-value]

    with pytest.raises(TypeError, match="expected ResponseEnvelope"):
        asyncio.run(my_tool())
```

The `test_decorator_calls_log_tool_call` test calls `my_tool()` and discards the coroutine — update it too:

```python
    @pytest.mark.unit
    def test_decorator_calls_log_tool_call(self) -> None:
        import asyncio

        @mcp_tool(sensitivity="medium")
        def my_tool() -> ResponseEnvelope:
            return ResponseEnvelope(
                summary=SummaryMeta(total_count=0, returned_count=0),
                data=[],
            )

        with patch("moneybin.mcp.decorator.log_tool_call") as mock_log:
            asyncio.run(my_tool())
            mock_log.assert_called_once()
            args = mock_log.call_args[0]
            assert args[0] == "my_tool"
            assert args[1] == Sensitivity.MEDIUM
```

- [ ] **Step 6: Run decorator + timeout tests together**

Run: `uv run pytest tests/moneybin/test_mcp/test_decorator.py tests/moneybin/test_mcp/test_tool_timeouts.py -v`
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/mcp/decorator.py tests/moneybin/test_mcp/test_tool_timeouts.py tests/moneybin/test_mcp/test_decorator.py
git commit -m "Wrap MCP tool dispatch in wall-clock timeout with DB reset"
```

---

## Task 5: Integration test — back-to-back call after timeout succeeds

**Files:**
- Modify: `tests/moneybin/test_mcp/test_tool_timeouts.py` (append integration test)

- [ ] **Step 1: Write the integration test**

Append to `tests/moneybin/test_mcp/test_tool_timeouts.py`:

```python
@pytest.mark.integration
def test_back_to_back_call_after_timeout_succeeds(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A call that times out must release the DB lock so the next call works."""
    import asyncio
    from unittest.mock import MagicMock

    import moneybin.database as db_module
    from moneybin.database import Database
    from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta

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

    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 5.0)
    second = asyncio.run(quick_tool())
    assert second.error is None
    assert second.data == [{"x": 42}]
```

- [ ] **Step 2: Run the integration test**

Run: `uv run pytest tests/moneybin/test_mcp/test_tool_timeouts.py -v -m integration`
Expected: PASS. The first call returns a `timed_out` envelope; the second call opens a fresh singleton, queries successfully.

- [ ] **Step 3: Commit**

```bash
git add tests/moneybin/test_mcp/test_tool_timeouts.py
git commit -m "Add integration test proving lock release after MCP timeout"
```

---

## Task 6: Update spec status, INDEX, README; run pre-commit

**Files:**
- Modify: `docs/specs/mcp-tool-timeouts.md` (status: draft → in-progress → implemented)
- Modify: `docs/specs/INDEX.md` (status row)
- Modify: `README.md` (roadmap icon if present; otherwise add a one-liner)

- [ ] **Step 1: Verify spec status block**

Read the top of `docs/specs/mcp-tool-timeouts.md`. Change:

```markdown
## Status

draft
```

to:

```markdown
## Status

implemented
```

- [ ] **Step 2: Update INDEX.md**

Run: `grep -n "mcp-tool-timeouts" docs/specs/INDEX.md`

If the spec is listed, update its status column to `implemented`. If not listed, add a row in the appropriate section with status `implemented` and a one-line description: "Wall-clock timeout + DB reset for every MCP tool dispatch."

- [ ] **Step 3: Update README**

Run: `grep -n "tool timeout\|MCP timeout\|timeout" README.md | head`

If a roadmap entry exists for MCP timeouts, change its icon to ✅. Otherwise add one line under the MCP "What Works Today" / infrastructure section: "Every MCP tool returns within a configurable wall-clock cap (default 30s); timeouts release the DuckDB lock so the next call succeeds."

- [ ] **Step 4: Format, lint, type-check**

Run: `make format && make lint && uv run pyright src/moneybin/mcp/decorator.py src/moneybin/database.py src/moneybin/errors.py src/moneybin/config.py`
Expected: clean.

- [ ] **Step 5: Full test sweep on touched modules**

Run: `uv run pytest tests/moneybin/test_mcp/ tests/moneybin/test_database_interrupt.py tests/moneybin/test_config.py -v`
Expected: all pass.

- [ ] **Step 6: Run /simplify on the diff before final commit**

Run the `/simplify` skill against the diff. Address any findings inline.

- [ ] **Step 7: Commit docs and any simplify follow-ups**

```bash
git add docs/specs/mcp-tool-timeouts.md docs/specs/INDEX.md README.md
git commit -m "Mark mcp-tool-timeouts spec implemented and update README"
```

- [ ] **Step 8: Final make check test**

Run: `make check test`
Expected: full pre-commit suite passes.

---

## Self-Review

**Spec coverage:**
- Req 1 (every dispatch wrapped, 30s default, settings): Task 1 + Task 4.
- Req 2 (interrupt + force-close on timeout): Task 2 + Task 4 (`interrupt_and_reset_database` invoked in timeout path).
- Req 3 (structured timeout envelope shape): Task 3 + Task 4 (`_build_timeout_envelope`).
- Req 4 (single low-cardinality log line, no PII): Task 4 (`logger.warning`, no args/kwargs in message); covered by `test_timeout_logs_low_cardinality_line`.
- Req 5 (next call succeeds): Task 5 (integration test).
- Req 6 (global cap, no per-tool overrides): Task 1 — single field.
- Req 7 (happy path unchanged): Task 4 — fast path is `await wait_for(...)` with no overhead beyond a single coroutine schedule; covered by `test_sync_tool_under_cap_passes_through`.

**Placeholder scan:** No "TBD"/"similar to"/"add appropriate" — every step shows the code or the exact command.

**Type consistency:** `interrupt_and_reset_database` named identically across Task 2 (definition), Task 4 (import), Task 5 (test). `_get_timeout_seconds` indirection introduced in Task 4 and only patched in tests — defined once. `tool_timeout_seconds` field name matches across config, decorator, env var derivation (`MONEYBIN_MCP__TOOL_TIMEOUT_SECONDS` per the existing `MONEYBIN_` + `__` convention).
