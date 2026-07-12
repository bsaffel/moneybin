"""Shared MCP elicitation primitives.

Elicitation is how a tool obtains explicit human agreement before an action
whose inference could be wrong (`.claude/rules/mcp.md`: "Destructive — use a
`confirm` parameter or elicitation; the AI must obtain explicit user
agreement"). Callers today: the first-run profile bootstrap
(``mcp/first_run.py``) and the three link-merge accept gates
(``mcp/tools/investments.py``, ``mcp/tools/accounts.py``,
``mcp/tools/merchants.py``) — so the capability probe and the confirm-or-raise
gate live here rather than being duplicated per call site.

The contract every caller must honor: a client that did NOT declare the
elicitation capability CANNOT be asked, so the guarded action must hard-fail
with an actionable error naming its CLI equivalent. It must never fall
through to acting.
"""

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
    """Build the structured refusal every ungranted confirmation raises."""
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
    """Obtain explicit human agreement for ``message``, or raise — never fall through.

    A review-queue proposal is by construction a weak inference (the resolver
    only proposes when it cannot bind unambiguously), so it is never eligible
    for agent self-accept regardless of confidence score
    (`.claude/rules/design-principles.md`, "Magic stays visible"). Without a
    human on the other end of an elicitation there is no way to act from MCP at
    all — the CLI is the way through, so the refusal names it.

    Args:
        message: The confirmation prompt the human reads. Names every entity the
            action touches and why it was proposed.
        subject: What needs confirming, as the subject of the refusal sentence
            (e.g. ``"This merge"``).
        unchanged: What remains true because nothing was written (e.g.
            ``"decision 'abc' is still pending"``).
        cli_equivalent: The exact CLI command that performs the same action.
        details: Structured error details (e.g. the decision id). A ``reason``
            key naming which gate refused is merged in.

    Raises:
        UserError: ``MUTATION_CONFIRMATION_REQUIRED`` when there is no MCP
            session, the client cannot elicit, or the human declined/cancelled.
    """
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
    result = await ctx.elicit(message, response_type=None)
    if not isinstance(result, AcceptedElicitation):
        raise _confirmation_unavailable(
            subject=subject,
            unchanged=unchanged,
            cli_equivalent=cli_equivalent,
            details=details,
            reason="declined",
            detail="the confirmation was declined or cancelled",
        )
