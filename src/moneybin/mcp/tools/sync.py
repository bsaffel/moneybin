"""sync_* tools — moneybin-server sync (login, connect, pull, status, schedule).

v2 MCP exposure per cli-restructure.md exposure principle. These tools
mirror the CLI's `moneybin sync *` surface; both are currently stubs
pending the sync-overview.md spec implementation. They return
not_implemented envelopes so the v2 taxonomy is complete and discoverable
to AI clients today.

Excluded from MCP exposure:
    - sync key rotate (passphrase material through LLM context window —
      security model violation per .claude/rules/mcp-server.md).
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

_SPEC = "docs/specs/sync-overview.md"


def _stub_envelope(action: str) -> ResponseEnvelope:
    """Return a uniform not_implemented envelope for sync stubs."""
    return build_envelope(
        data={"status": "not_implemented", "action": action, "spec": _SPEC},
        sensitivity="low",
        actions=[f"See {_SPEC} for the design"],
    )


@mcp_tool(sensitivity="low")
def sync_login() -> ResponseEnvelope:
    """Initiate device-code OAuth login with moneybin-server."""
    return _stub_envelope("sync_login")


@mcp_tool(sensitivity="low")
def sync_logout() -> ResponseEnvelope:
    """Clear stored JWT for moneybin-server."""
    return _stub_envelope("sync_logout")


@mcp_tool(sensitivity="low")
def sync_connect(institution: str | None = None) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Initiate OAuth connection to a bank/aggregator."""
    return _stub_envelope("sync_connect")


@mcp_tool(sensitivity="low")
def sync_disconnect(institution: str) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Remove an institution; idempotent."""
    return _stub_envelope("sync_disconnect")


@mcp_tool(sensitivity="low")
def sync_pull(
    institution: str | None = None,  # noqa: ARG001 — placeholder
    force: bool = False,  # noqa: ARG001 — placeholder
) -> ResponseEnvelope:
    """Pull data from connected institutions."""
    return _stub_envelope("sync_pull")


@mcp_tool(sensitivity="low")
def sync_status() -> ResponseEnvelope:
    """Connected institutions, last-sync times, and errors."""
    return _stub_envelope("sync_status")


@mcp_tool(sensitivity="low")
def sync_schedule_set(time: str) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Install a daily sync at the given HH:MM."""
    return _stub_envelope("sync_schedule_set")


@mcp_tool(sensitivity="low")
def sync_schedule_show() -> ResponseEnvelope:
    """Show current scheduled sync details."""
    return _stub_envelope("sync_schedule_show")


@mcp_tool(sensitivity="low")
def sync_schedule_remove() -> ResponseEnvelope:
    """Uninstall the scheduled sync job."""
    return _stub_envelope("sync_schedule_remove")


def register_sync_tools(mcp: FastMCP) -> None:
    """Register all sync namespace tools with the FastMCP server."""
    for fn, desc in [
        (sync_login, "Initiate device-code OAuth login with moneybin-server."),
        (sync_logout, "Clear stored moneybin-server JWT."),
        (sync_connect, "Initiate OAuth connection to a bank or aggregator."),
        (sync_disconnect, "Remove an institution; idempotent."),
        (sync_pull, "Pull data from connected institutions."),
        (sync_status, "Connected institutions, last-sync times, and errors."),
        (sync_schedule_set, "Install a daily sync at HH:MM."),
        (sync_schedule_show, "Show current scheduled sync."),
        (sync_schedule_remove, "Uninstall the scheduled sync job."),
    ]:
        register(mcp, fn, fn.__name__, desc)
