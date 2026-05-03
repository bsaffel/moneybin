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
            except TimeoutError:
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
