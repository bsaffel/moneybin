"""moneybin.discover — per-session progressive disclosure via tag enablement."""

from __future__ import annotations

import logging

from fastmcp import Context, FastMCP
from fastmcp.server.transforms.visibility import enable_components

from moneybin.errors import UserError
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)

logger = logging.getLogger(__name__)


_NAMESPACE_DESCRIPTIONS: dict[str, str] = {
    "categorize": "Rules, merchant mappings, bulk categorization",
    "budget": "Budget targets, status, rollovers",
    "tax": "W-2 data, deductible expense search",
    "privacy": "Consent status, grants, revocations, audit log",
    "transactions.matches": "Match review workflow",
}


@mcp_tool(sensitivity="low")
async def moneybin_discover(domain: str, ctx: Context) -> ResponseEnvelope:
    """Reveal tools from an extended namespace for the calling session.

    Extended namespaces (categorize, budget, tax, privacy,
    transactions.matches) start hidden. Calling this tool with a domain
    name enables the tools tagged with that domain for the current session
    only — other connected clients are unaffected.

    Args:
        domain: The namespace to reveal (e.g. 'categorize', 'budget').
        ctx: FastMCP request context (auto-injected). Used to scope the
            visibility change to the calling session only.
    """
    from moneybin.mcp.server import EXTENDED_DOMAINS

    if domain not in EXTENDED_DOMAINS:
        known = ", ".join(sorted(EXTENDED_DOMAINS))
        return build_error_envelope(
            error=UserError(
                f"Unknown domain: {domain}",
                code="unknown_domain",
                hint=f"Known extended namespaces: {known}",
            ),
            sensitivity="low",
        )

    await enable_components(ctx, tags={domain})
    return build_envelope(
        data={
            "domain": domain,
            "description": _NAMESPACE_DESCRIPTIONS.get(domain, ""),
        },
        sensitivity="low",
        actions=[
            f"Tools tagged '{domain}' enabled for this session.",
            "Call discover again with a different domain to reveal more tools.",
        ],
    )


def register_discover_tool(mcp: FastMCP) -> None:
    """Register moneybin.discover with the server (always visible — no domain tag)."""
    mcp.tool(
        name="moneybin.discover",
        description=(
            "Reveal tools from an extended namespace (categorize, budget, "
            "tax, privacy, transactions.matches) for the current session."
        ),
    )(moneybin_discover)
