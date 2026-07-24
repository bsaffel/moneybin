"""Shared MCP elicitation primitives."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastmcp.server.dependencies import get_context
from fastmcp.server.elicitation import AcceptedElicitation
from mcp.types import ClientCapabilities, ElicitationCapability

from moneybin import error_codes
from moneybin.errors import UserError

if TYPE_CHECKING:
    from fastmcp.server.context import Context


def supports_elicitation(ctx: Context) -> bool:
    """True when the connected client declared the elicitation capability."""
    return ctx.session.check_client_capability(
        ClientCapabilities(elicitation=ElicitationCapability())
    )


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
    result = await ctx.elicit(
        message,
        response_type=bool,
        response_title="Confirm inferred financial behavior",
        response_description=(
            "Select true only after reviewing the inference and affected data."
        ),
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
