"""moneybin_discover — per-session progressive disclosure via tag enablement."""

from __future__ import annotations

from fastmcp import Context, FastMCP
from fastmcp.server.transforms.visibility import enable_components

from moneybin.errors import UserError
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)


@mcp_tool(sensitivity="low")
async def moneybin_discover(
    ctx: Context, domain: str | None = None
) -> ResponseEnvelope:
    """List or enable extended-namespace tool domains for the calling session.

    Extended namespaces (categorize, budget, tax, privacy,
    transactions_matches) start hidden when progressive disclosure is on.
    Call with no ``domain`` to enumerate the available namespaces and their
    descriptions; call with a ``domain`` to enable that namespace's tools
    for the current session only (other connected clients are unaffected).

    Args:
        domain: Optional namespace to enable. If omitted, the tool returns
            the catalog of available domains so the agent can choose one.
        ctx: FastMCP request context (auto-injected). Used to scope the
            visibility change to the calling session only.
    """
    from moneybin.mcp.server import EXTENDED_DOMAIN_DESCRIPTIONS, EXTENDED_DOMAINS

    if domain is None:
        catalog = [
            {"domain": name, "description": EXTENDED_DOMAIN_DESCRIPTIONS[name]}
            for name in sorted(EXTENDED_DOMAINS)
        ]
        return build_envelope(
            data=catalog,
            sensitivity="low",
            actions=[
                "Call moneybin_discover(domain='<name>') with one of the listed "
                "domains to enable its tools for this session.",
            ],
        )

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
            "description": EXTENDED_DOMAIN_DESCRIPTIONS.get(domain, ""),
        },
        sensitivity="low",
        actions=[
            f"Tools tagged '{domain}' enabled for this session.",
            "Call discover again with a different domain to reveal more tools.",
        ],
    )


def register_discover_tool(mcp: FastMCP) -> None:
    """Register moneybin_discover with the server (always visible — no domain tag)."""
    from moneybin.mcp.server import EXTENDED_DOMAINS

    domains = ", ".join(sorted(EXTENDED_DOMAINS))
    mcp.tool(
        name="moneybin_discover",
        description=(
            "List or enable extended-namespace tool domains. "
            f"Available: {domains}. "
            "Call with no arguments to enumerate; pass `domain=<name>` to "
            "enable that namespace's tools for this session."
        ),
    )(moneybin_discover)
