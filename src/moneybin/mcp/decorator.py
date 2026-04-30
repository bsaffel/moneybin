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

from moneybin.errors import classify_user_error
from moneybin.mcp.privacy import Sensitivity, log_tool_call
from moneybin.protocol.envelope import ResponseEnvelope, build_error_envelope

logger = logging.getLogger(__name__)


def mcp_tool(
    *,
    sensitivity: str,
    domain: str | None = None,
) -> Callable[..., Any]:
    """Decorator that marks a function as an MCP tool with a sensitivity tier.

    Catches classified domain exceptions (UserError, DatabaseKeyError,
    FileNotFoundError) and converts them to error ResponseEnvelope values.
    All other exceptions propagate to fastmcp's server-level masking.

    Args:
        sensitivity: Data sensitivity tier (``"low"``, ``"medium"``, ``"high"``).
        domain: Extended-namespace name (e.g. ``"categorize"``, ``"budget"``).
            Tools with a ``domain`` start hidden and must be enabled per-session
            via ``moneybin.discover``. Tools without a domain are core tools,
            visible at connect. The registration layer translates this into
            ``mcp.tool(tags={domain})``; a server-level ``Visibility`` transform
            then hides the tagged set.

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
            except Exception as exc:
                classified = classify_user_error(exc)
                if classified is None:
                    raise
                logger.error(
                    f"Tool {fn.__name__} raised {type(exc).__name__}: {classified.code}"
                )
                return build_error_envelope(error=classified, sensitivity="low")
            if not isinstance(result, ResponseEnvelope):
                raise TypeError(
                    f"{fn.__name__} returned {type(result).__name__},"
                    " expected ResponseEnvelope"
                )
            return result

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        wrapper._mcp_domain = domain  # type: ignore[attr-defined]
        return wrapper

    return decorator
