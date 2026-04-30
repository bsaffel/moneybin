# src/moneybin/mcp/decorator.py
"""MCP tool decorator with sensitivity tier, privacy middleware, and error handling.

Wraps tool functions with:
1. Sensitivity logging via the privacy middleware stub
2. Domain exception classification into error ResponseEnvelope
3. Return of ResponseEnvelope directly (fastmcp 3.x serializes Pydantic models
   into both content and structured_content)
4. Tool name tracking for audit/debugging

Usage::

    @mcp_tool(sensitivity="medium")
    def spending_summary(months: int = 3) -> ResponseEnvelope:
        service = SpendingService(get_database())
        return service.summary(months).to_envelope()

The decorator does NOT register the tool with FastMCP — that happens
in the namespace registry. This separation lets us control which tools
are registered at connection time vs on-demand.

Classified exceptions (UserError, DatabaseKeyError, FileNotFoundError) are
caught and converted to error envelopes so all surfaces (MCP, CLI --output json,
future HTTP) return a consistent envelope shape. All other exceptions propagate
so fastmcp's mask_error_details wraps them into masked ToolErrors.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any

from moneybin.database import DatabaseKeyError
from moneybin.errors import UserError
from moneybin.mcp.privacy import Sensitivity, log_tool_call
from moneybin.protocol.envelope import ResponseEnvelope, build_error_envelope

logger = logging.getLogger(__name__)


def mcp_tool(
    *,
    sensitivity: str,
) -> Callable[..., Any]:
    """Decorator that marks a function as an MCP tool with a sensitivity tier.

    Catches classified domain exceptions (UserError, DatabaseKeyError,
    FileNotFoundError) and converts them to error ResponseEnvelope values.
    All other exceptions propagate to fastmcp's server-level masking.

    Args:
        sensitivity: Data sensitivity tier (``"low"``, ``"medium"``, ``"high"``).

    Returns:
        Decorator that wraps the function with privacy logging, error
        classification, and direct ResponseEnvelope return.
    """
    tier = Sensitivity(sensitivity)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> ResponseEnvelope:
            log_tool_call(fn.__name__, tier)
            try:
                result = fn(*args, **kwargs)
            except UserError as exc:
                logger.error(f"Tool {fn.__name__} raised UserError: {exc.code}")
                return build_error_envelope(error=exc, sensitivity="low")
            except DatabaseKeyError as exc:
                logger.error(f"Tool {fn.__name__} raised DatabaseKeyError")
                return build_error_envelope(
                    error=UserError(str(exc), code="DATABASE_KEY_ERROR"),
                    sensitivity="low",
                )
            except FileNotFoundError as exc:
                logger.error(f"Tool {fn.__name__} raised FileNotFoundError")
                msg = f"{exc.strerror}: {exc.filename}" if exc.filename else str(exc)
                return build_error_envelope(
                    error=UserError(msg, code="FILE_NOT_FOUND"),
                    sensitivity="low",
                )
            # Unclassified exceptions propagate; fastmcp's mask_error_details
            # wraps them into masked ToolErrors at the server boundary.
            if not isinstance(result, ResponseEnvelope):
                raise TypeError(
                    f"{fn.__name__} returned {type(result).__name__},"
                    " expected ResponseEnvelope"
                )
            return result

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        return wrapper

    return decorator
