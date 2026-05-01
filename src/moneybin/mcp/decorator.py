"""MCP tool decorator: sensitivity logging, error classification, envelope guard.

Classified exceptions (``UserError``, ``DatabaseKeyError``, ``FileNotFoundError``)
become error envelopes so every surface — MCP, CLI ``--output json``, future
HTTP — returns a consistent shape. Anything else propagates to the server's
``mask_error_details`` boundary. Registration of the wrapped function with
FastMCP happens separately in ``moneybin.mcp._registration``.
"""

from __future__ import annotations

import functools
import inspect
import logging
from collections.abc import Callable
from typing import Any, Literal

from moneybin.errors import classify_user_error
from moneybin.mcp.privacy import Sensitivity, log_tool_call
from moneybin.protocol.envelope import ResponseEnvelope, build_error_envelope

logger = logging.getLogger(__name__)


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


def mcp_tool(
    *,
    sensitivity: Literal["low", "medium", "high"],
    domain: str | None = None,
) -> Callable[..., Any]:
    """Mark a function as an MCP tool with a sensitivity tier and optional domain.

    Tools with a ``domain`` start hidden; ``moneybin.discover`` enables them
    per-session via FastMCP tag visibility.
    """
    tier = Sensitivity(sensitivity)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope:
                log_tool_call(fn.__name__, tier)
                try:
                    result = await fn(*args, **kwargs)
                except Exception as exc:
                    return _classify_or_raise(fn.__name__, exc)
                return _check_envelope(fn.__name__, result)

            async_wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
            async_wrapper._mcp_domain = domain  # type: ignore[attr-defined]
            return async_wrapper

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope:
            log_tool_call(fn.__name__, tier)
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:
                return _classify_or_raise(fn.__name__, exc)
            return _check_envelope(fn.__name__, result)

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        wrapper._mcp_domain = domain  # type: ignore[attr-defined]
        return wrapper

    return decorator
