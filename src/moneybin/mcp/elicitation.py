"""Shared MCP elicitation primitives."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TYPE_CHECKING

from fastmcp.server.dependencies import get_context
from fastmcp.server.elicitation import AcceptedElicitation
from mcp.types import ClientCapabilities, ElicitationCapability

from moneybin import error_codes
from moneybin.config import DEFAULT_ELICITATION_WAIT_SECONDS, get_settings
from moneybin.errors import UserError
from moneybin.mcp.decorator import excluded_from_tool_deadline

if TYPE_CHECKING:
    from fastmcp.server.context import Context


def supports_elicitation(ctx: Context) -> bool:
    """True when the connected client declared the elicitation capability."""
    return ctx.session.check_client_capability(
        ClientCapabilities(elicitation=ElicitationCapability())
    )


def elicitation_wait_seconds() -> float:
    """Read the configured human-answer budget. Indirected for monkeypatching."""
    try:
        return get_settings().mcp.elicitation_wait_seconds
    except RuntimeError:
        return DEFAULT_ELICITATION_WAIT_SECONDS


async def elicit_bounded[T](elicitation: Awaitable[T]) -> T | None:
    """Await one elicitation, bounded by the human-answer budget.

    Takes the ``ctx.elicit(...)`` coroutine itself so the caller keeps its
    precise result type. Returns ``None`` — never a real elicitation result —
    when nobody answered in time, so every caller must choose its own degraded
    path instead of inheriting a default. The tool's wall-clock cap is paused
    for the wait; see ``excluded_from_tool_deadline`` for why a human's
    deliberation is not machine work.
    """
    try:
        with excluded_from_tool_deadline():
            return await asyncio.wait_for(elicitation, elicitation_wait_seconds())
    except TimeoutError:
        return None


def _confirmation_unavailable(
    *,
    subject: str,
    unchanged: str,
    cli_equivalent: str,
    details: dict[str, str],
    reason: str,
    detail: str,
) -> UserError:
    """Build the structured refusal for an ungranted inference."""
    return UserError(
        f"{subject} needs explicit confirmation and {detail}. Nothing was "
        f"written; {unchanged}.",
        code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
        hint=f"Accept it from a terminal instead: `{cli_equivalent}`",
        details={**details, "reason": reason},
    )


async def confirm_or_raise(
    message: str,
    *,
    subject: str,
    unchanged: str,
    cli_equivalent: str,
    details: dict[str, str],
) -> None:
    """Obtain explicit human agreement for an inferred financial behavior."""
    try:
        ctx = get_context()
    except RuntimeError as exc:
        raise _confirmation_unavailable(
            subject=subject,
            unchanged=unchanged,
            cli_equivalent=cli_equivalent,
            details=details,
            reason="no_session",
            detail="there is no active MCP session to ask",
        ) from exc
    if not supports_elicitation(ctx):
        raise _confirmation_unavailable(
            subject=subject,
            unchanged=unchanged,
            cli_equivalent=cli_equivalent,
            details=details,
            reason="client_unsupported",
            detail="this client cannot prompt you (no elicitation)",
        )
    result = await elicit_bounded(
        ctx.elicit(
            message,
            response_type=bool,
            response_title="Confirm inferred financial behavior",
            response_description=(
                "Select true only after reviewing the inference and affected data."
            ),
        )
    )
    if result is None:
        raise _confirmation_unavailable(
            subject=subject,
            unchanged=unchanged,
            cli_equivalent=cli_equivalent,
            details=details,
            reason="timeout",
            detail="nobody answered the prompt in time",
        )
    if not (isinstance(result, AcceptedElicitation) and result.data is True):
        raise _confirmation_unavailable(
            subject=subject,
            unchanged=unchanged,
            cli_equivalent=cli_equivalent,
            details=details,
            reason="declined",
            detail="the confirmation was declined or cancelled",
        )
