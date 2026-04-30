# src/moneybin/mcp/decorator.py
"""MCP tool decorator with sensitivity tier and privacy middleware.

Wraps tool functions with:
1. Sensitivity logging via the privacy middleware stub
2. Automatic JSON serialization of ResponseEnvelope returns
3. Tool name tracking for audit/debugging

Usage::

    @mcp_tool(sensitivity="medium")
    def spending_summary(months: int = 3) -> ResponseEnvelope:
        service = SpendingService(get_database())
        return service.summary(months).to_envelope()

The decorator does NOT register the tool with FastMCP — that happens
in the namespace registry. This separation lets us control which tools
are registered at connection time vs on-demand.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any

from moneybin.mcp.privacy import Sensitivity, log_tool_call
from moneybin.protocol.envelope import ResponseEnvelope

logger = logging.getLogger(__name__)


def mcp_tool(
    *,
    sensitivity: str,
) -> Callable[..., Any]:
    """Decorator that marks a function as an MCP tool with a sensitivity tier.

    Args:
        sensitivity: Data sensitivity tier (``"low"``, ``"medium"``, ``"high"``).

    Returns:
        Decorator that wraps the function with privacy logging and
        envelope serialization.
    """
    tier = Sensitivity(sensitivity)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            log_tool_call(fn.__name__, tier)
            result = fn(*args, **kwargs)
            if isinstance(result, ResponseEnvelope):
                return result.to_json()
            return result

        wrapper._mcp_sensitivity = sensitivity  # type: ignore[attr-defined]
        return wrapper

    return decorator
