"""Shared MCP elicitation primitives.

Elicitation is how a tool obtains explicit human agreement before an action
whose inference could be wrong (`.claude/rules/mcp.md`: "Destructive — use a
`confirm` parameter or elicitation; the AI must obtain explicit user
agreement"). Two callers today — the first-run profile bootstrap
(``mcp/first_run.py``) and the security-merge accept gate
(``mcp/tools/investments.py``) — so the capability probe lives here rather
than being duplicated per call site.

The contract every caller must honor: a client that did NOT declare the
elicitation capability CANNOT be asked, so the guarded action must hard-fail
with an actionable error naming its CLI equivalent. It must never fall
through to acting.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mcp.types import ClientCapabilities, ElicitationCapability

if TYPE_CHECKING:
    from fastmcp.server.context import Context


def supports_elicitation(ctx: Context) -> bool:
    """True when the connected client declared the elicitation capability."""
    return ctx.session.check_client_capability(
        ClientCapabilities(elicitation=ElicitationCapability())
    )
