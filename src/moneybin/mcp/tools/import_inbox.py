"""Inbox MCP tools — drain and preview the watched import folder."""

from __future__ import annotations

import dataclasses
import logging

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.inbox_service import InboxService

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low")
def inbox_sync() -> ResponseEnvelope:
    """Drain the active profile's import inbox."""
    service = InboxService.for_active_profile()
    result = dataclasses.asdict(service.sync())

    actions: list[str] = ["Use transactions.search to view newly imported transactions"]
    if result["failed"]:
        actions.insert(
            0,
            "Move failed files into inbox/<account-slug>/ and re-run import_inbox_sync",
        )
    return build_envelope(data=result, sensitivity="low", actions=actions)


@mcp_tool(sensitivity="low")
def inbox_list() -> ResponseEnvelope:
    """Preview the active profile's inbox without moving anything."""
    service = InboxService.for_active_profile_no_db()
    result = dataclasses.asdict(service.enumerate())
    return build_envelope(
        data=result,
        sensitivity="low",
        actions=["Use import_inbox_sync to drain the inbox"],
    )


def register_inbox_tools(mcp: FastMCP) -> None:
    """Register the two inbox tools with the MCP server."""
    register(
        mcp,
        inbox_sync,
        "import_inbox_sync",
        "Drain the active profile's import inbox; move successes to "
        "processed/ and failures to failed/ with structured error sidecars.",
    )
    register(
        mcp,
        inbox_list,
        "import_inbox_list",
        "Preview the active profile's import inbox without moving anything.",
    )
