"""Inbox MCP tools — drain and preview the watched import folder."""

from __future__ import annotations

import dataclasses
import logging

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.inbox_service import InboxService

logger = logging.getLogger(__name__)


def _uncategorized_count() -> int:
    """Return the count of transactions lacking a category entry.

    Returns 0 on any error so a DB hiccup never breaks the import summary.
    """
    try:
        from moneybin.tables import FCT_TRANSACTIONS, TRANSACTION_CATEGORIES

        with get_database(read_only=True) as db:
            row = db.execute(
                f"""
                SELECT COUNT(*)
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} tc USING (transaction_id)
                WHERE tc.transaction_id IS NULL
                """,  # noqa: S608  # table names are TableRef constants, not user input
            ).fetchone()
        return int(row[0]) if row else 0
    except Exception:  # noqa: BLE001 — never surface DB errors in summary hint
        return 0


@mcp_tool(sensitivity="low", read_only=False, idempotent=False)
def inbox_sync() -> ResponseEnvelope:
    """Drain the active profile's import inbox."""
    from moneybin.config import get_settings

    with get_database() as db:
        service = InboxService(db=db, settings=get_settings())
        result = dataclasses.asdict(service.sync())

    actions: list[str] = ["Use transactions.search to view newly imported transactions"]
    if result["failed"]:
        actions.insert(
            0,
            "Move failed files into inbox/<account-slug>/ and re-run import_inbox_sync",
        )

    threshold = get_settings().categorization.assist_offer_threshold
    uncategorized = _uncategorized_count()
    if uncategorized >= threshold:
        actions.append(
            f"{uncategorized} uncategorized transactions — use "
            "moneybin_discover('categorize') then transactions_categorize_assist "
            "for AI-assisted categorization, or "
            "`moneybin transactions categorize export-uncategorized` for the CLI bridge"
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
        "processed/ and failures to failed/ with structured error sidecars. "
        "Writes to raw.* source tables and moves files within the inbox directory; revert by manually moving processed files back into inbox/<account-slug>/ and accepting that already-imported source rows are deduplicated on the next sync.",
    )
    register(
        mcp,
        inbox_list,
        "import_inbox_list",
        "Preview the active profile's import inbox without moving anything.",
    )
