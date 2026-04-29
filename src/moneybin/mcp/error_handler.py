"""MCP-side adapter for cross-cutting user-facing error classification.

Wraps a tool function so that classified exceptions become an error
``ResponseEnvelope`` (serialized to JSON like any other tool return)
rather than propagating to the framework. Unclassified exceptions
re-raise so genuine programmer errors still surface as 500-equivalent
failures rather than being silently translated into user-facing text.

Usage::

    @mcp_tool(sensitivity="medium")
    @handle_mcp_errors
    def categorize_bulk(items: list[dict]) -> ResponseEnvelope:
        ...

``handle_mcp_errors`` should sit *below* ``@mcp_tool`` so the envelope
returned on error still flows through the decorator's JSON serialization.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any

from moneybin.errors import classify_user_error
from moneybin.mcp.envelope import ResponseEnvelope, build_error_envelope

logger = logging.getLogger(__name__)


def handle_mcp_errors(fn: Callable[..., ResponseEnvelope]) -> Callable[..., Any]:
    """Translate classified exceptions into an error ``ResponseEnvelope``.

    The wrapped function continues to return its normal envelope on
    success. On a classified exception, the wrapper returns an error
    envelope and logs the message at error level. Unclassified exceptions
    are re-raised so the framework can surface them as failures.
    """

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            user_error = classify_user_error(exc)
            if user_error is None:
                raise
            logger.error(f"❌ {fn.__name__}: {user_error.message}")
            return build_error_envelope(error=user_error)

    return wrapper
