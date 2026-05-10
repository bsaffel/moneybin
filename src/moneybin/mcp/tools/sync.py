"""sync_* stubs — pending docs/specs/sync-overview.md.

Excludes sync key rotate from MCP exposure (passphrase material through
the LLM context window is a security model violation).
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, not_implemented_envelope

_SPEC = "docs/specs/sync-overview.md"


def _stub(action: str) -> ResponseEnvelope:
    return not_implemented_envelope(action=action, spec=_SPEC)


@mcp_tool(sensitivity="low", read_only=False, idempotent=False, open_world=True)
def sync_login() -> ResponseEnvelope:
    """Initiate device-code OAuth login with moneybin-server."""
    return _stub("sync_login")


@mcp_tool(sensitivity="low", read_only=False)
def sync_logout() -> ResponseEnvelope:
    """Clear stored JWT for moneybin-server."""
    return _stub("sync_logout")


@mcp_tool(sensitivity="low", read_only=False, idempotent=False, open_world=True)
def sync_connect(institution: str | None = None) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Initiate OAuth connection to a bank/aggregator."""
    return _stub("sync_connect")


@mcp_tool(sensitivity="low", read_only=False, open_world=True)
def sync_disconnect(institution: str) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Remove an institution; idempotent."""
    return _stub("sync_disconnect")


@mcp_tool(sensitivity="low", read_only=False, idempotent=False, open_world=True)
def sync_pull(
    institution: str | None = None,  # noqa: ARG001 — placeholder
    force: bool = False,  # noqa: ARG001 — placeholder
) -> ResponseEnvelope:
    """Pull data from connected institutions."""
    return _stub("sync_pull")


@mcp_tool(sensitivity="low")
def sync_status() -> ResponseEnvelope:
    """Connected institutions, last-sync times, and errors."""
    return _stub("sync_status")


@mcp_tool(sensitivity="low", read_only=False)
def sync_schedule_set(time: str) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Install a daily sync at the given HH:MM."""
    return _stub("sync_schedule_set")


@mcp_tool(sensitivity="low")
def sync_schedule_show() -> ResponseEnvelope:
    """Show current scheduled sync details."""
    return _stub("sync_schedule_show")


@mcp_tool(sensitivity="low", read_only=False)
def sync_schedule_remove() -> ResponseEnvelope:
    """Uninstall the scheduled sync job."""
    return _stub("sync_schedule_remove")


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
