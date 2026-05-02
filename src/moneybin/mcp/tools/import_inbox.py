"""Inbox MCP tools — drain and preview the watched import folder."""

from __future__ import annotations

import dataclasses
import logging
from typing import TYPE_CHECKING

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

if TYPE_CHECKING:
    from moneybin.services.inbox_service import InboxService

logger = logging.getLogger(__name__)


def _build_service() -> InboxService:
    """Indirection for tests to monkeypatch."""
    from moneybin.config import get_settings
    from moneybin.database import get_database
    from moneybin.services.inbox_service import InboxService

    return InboxService(db=get_database(), settings=get_settings())


def _build_service_no_db() -> InboxService:
    """Same as _build_service but without opening the database.

    Used by inbox_list, which only enumerates the filesystem and does not
    need DB access. Lets the tool work even when the DB is locked or its
    encryption key is unavailable (e.g., onboarding/recovery flows).
    """
    from moneybin.config import get_settings
    from moneybin.services.inbox_service import InboxService

    return InboxService(db=None, settings=get_settings())


@mcp_tool(sensitivity="low")
def inbox_sync() -> ResponseEnvelope:
    """Drain the active profile's import inbox."""
    service = _build_service()
    result = dataclasses.asdict(service.sync())

    actions: list[str] = ["Use transactions.search to view newly imported transactions"]
    if result["failed"]:
        actions.insert(
            0,
            "Move failed files into inbox/<account-slug>/ and re-run import.inbox_sync",
        )
    return build_envelope(data=result, sensitivity="low", actions=actions)


@mcp_tool(sensitivity="low")
def inbox_list() -> ResponseEnvelope:
    """Preview the active profile's inbox without moving anything."""
    service = _build_service_no_db()
    result = dataclasses.asdict(service.enumerate())
    return build_envelope(
        data=result,
        sensitivity="low",
        actions=["Use import.inbox_sync to drain the inbox"],
    )


def register_inbox_tools(mcp: FastMCP) -> None:
    """Register the two inbox tools with the MCP server."""
    register(
        mcp,
        inbox_sync,
        "import.inbox_sync",
        "Drain the active profile's import inbox; move successes to "
        "processed/ and failures to failed/ with structured error sidecars.",
    )
    register(
        mcp,
        inbox_list,
        "import.inbox_list",
        "Preview the active profile's import inbox without moving anything.",
    )
