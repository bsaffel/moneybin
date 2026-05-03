"""MCP tool decorator: sensitivity logging, timeout guard, error classification, envelope guard.

Every decorated tool is exposed as an async coroutine — FastMCP awaits it,
and sync tool bodies run via ``asyncio.to_thread``. On timeout the
decorator drops the singleton DuckDB connection so the next call reopens
a fresh one, releasing any held write lock. Classified domain exceptions
become error envelopes here; anything else propagates to the server's
``mask_error_details`` boundary.

Known limitation — sync tool body continues after timeout: ``asyncio.timeout()``
cancels the awaited task, but the OS thread running the sync body keeps
going until it naturally returns. A tool that mutates filesystem or DB state
(e.g., ``import_inbox_sync``) may finish that work in the background after
the client has already received a ``timed_out`` envelope. Clients that
retry can produce duplicate or conflicting writes. The contract here is
"release the lock and respond within the cap," not "guarantee the
underlying work was undone." Tools doing non-idempotent writes that risk
exceeding the cap must be redesigned (e.g., decomposed into smaller per-
unit calls) — see ``docs/specs/mcp-tool-timeouts.md`` Out of Scope.
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
        # mask_error_details=True at the server boundary swallows the TypeError
        # into a generic ToolError, so log the contract violation first.
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
        if inspect.isasyncgenfunction(fn):
            raise TypeError(
                f"{fn.__name__} is an async generator — MCP tools must be regular coroutines or sync functions"
            )
        if inspect.isgeneratorfunction(fn):
            raise TypeError(
                f"{fn.__name__} is a sync generator — MCP tools must return ResponseEnvelope directly"
            )

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope:
            log_tool_call(fn.__name__, tier)
            timeout_s = _get_timeout_seconds()
            started = time.monotonic()
            # asyncio.timeout()'s .expired() lets us distinguish a cap-fired
            # TimeoutError from one the tool body raised itself (e.g., a
            # downstream HTTP timeout). Without this, both surface as
            # TimeoutError and would be misclassified as cap-fired, causing
            # spurious DB resets and misleading "timed_out" envelopes.
            cm = asyncio.timeout(timeout_s)
            try:
                async with cm:
                    if is_coro:
                        result = await fn(*args, **kwargs)
                    else:
                        result = await asyncio.to_thread(fn, *args, **kwargs)
            except TimeoutError as exc:
                if not cm.expired():
                    return _classify_or_raise(fn.__name__, exc)
                elapsed = time.monotonic() - started
                logger.warning(
                    f"Tool {fn.__name__} timed out after {elapsed:.2f}s "
                    f"(cap {timeout_s:.1f}s); interrupting DB and resetting connection"
                )
                try:
                    interrupt_and_reset_database()
                except Exception as cleanup_exc:  # noqa: BLE001 — cleanup must not raise
                    logger.error(
                        f"interrupt_and_reset_database failed during {fn.__name__} "
                        f"timeout cleanup: {type(cleanup_exc).__name__}"
                    )
                # Brief grace period: the surviving sync-tool thread needs a
                # tick to see the closed DuckDB connection, raise, and unwind
                # any per-tool resources (e.g. InboxService.acquire_lock's
                # flock). Without this, an immediate retry hits inbox_busy
                # while the previous thread is still in its finally block.
                await asyncio.sleep(0.5)
                return _build_timeout_envelope(fn.__name__, elapsed, timeout_s)
            except Exception as exc:
                return _classify_or_raise(fn.__name__, exc)
            return _check_envelope(fn.__name__, result)

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        wrapper._mcp_domain = domain  # type: ignore[attr-defined]
        return wrapper

    return decorator
