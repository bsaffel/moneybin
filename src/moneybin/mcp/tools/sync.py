"""sync_* MCP tools — Phase 1 implementations.

Per docs/specs/2026-05-13-plaid-sync-design.md Section 11.

Excluded from MCP (CLI-only): sync_login, sync_logout (browser interaction +
credential handling). sync_key_rotate (Phase 3 stub; passphrase material is
CLI-only by convention).
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    not_implemented_envelope,
)

_SPEC = "docs/specs/2026-05-13-plaid-sync-design.md"


def _stub(action: str) -> ResponseEnvelope:
    cli_verb = action.removeprefix("sync_").replace("_", " ")
    return not_implemented_envelope(
        action=action,
        spec=_SPEC,
        actions=[
            f"Use the CLI: moneybin sync {cli_verb}",
            f"See {_SPEC} for the planned MCP surface",
        ],
    )


@contextmanager
def _build_sync_service() -> Generator[Any, None, None]:
    """Context manager yielding a SyncService with active Database connection."""
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.connectors.sync_client import SyncClient  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.loaders.plaid_loader import PlaidLoader  # noqa: PLC0415
    from moneybin.services.sync_service import SyncService  # noqa: PLC0415

    settings = get_settings()
    client = SyncClient(server_url=str(settings.sync.server_url))
    with get_database(read_only=False) as db:
        loader = PlaidLoader(db)
        yield SyncService(client=client, db=db, loader=loader)


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False, open_world=True)
def sync_pull(
    institution: str | None = None,
    force: bool = False,
) -> ResponseEnvelope:
    """Pull transactions, accounts, balances from connected institutions via moneybin-server.

    Amounts in loaded data follow MoneyBin accounting convention: negative = expense,
    positive = income; the Plaid sign flip happens during ingestion. Returns per-institution
    results including error_code for any failed institutions. Mutates raw.plaid_* tables and
    propagates through SQLMesh to core; idempotent on re-run (transactions upsert by
    (transaction_id, provider_item_id)).
    """
    with _build_sync_service() as service:
        result = service.pull(institution=institution, force=force)
    return build_envelope(
        data=result.model_dump(mode="json"),
        sensitivity="medium",
        actions=["Use sync_status to see connection health going forward."],
    )


@mcp_tool(sensitivity="low")
def sync_status() -> ResponseEnvelope:
    """Connected institutions, last-sync times, and error-state guidance."""
    with _build_sync_service() as service:
        connections = service.list_connections()
    return build_envelope(
        data=[c.model_dump(mode="json") for c in connections],
        sensitivity="low",
        actions=[],
    )


@mcp_tool(sensitivity="low", read_only=False, idempotent=False, open_world=True)
def sync_connect(institution: str | None = None) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Initiate OAuth connection to a bank/aggregator."""
    return _stub("sync_connect")


@mcp_tool(sensitivity="low", read_only=False, open_world=True)
def sync_disconnect(institution: str) -> ResponseEnvelope:  # noqa: ARG001 — placeholder
    """Remove an institution; idempotent."""
    return _stub("sync_disconnect")


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
        (sync_connect, "Initiate OAuth connection to a bank or aggregator."),
        (sync_disconnect, "Remove an institution; idempotent."),
        (sync_pull, "Pull data from connected institutions."),
        (sync_status, "Connected institutions, last-sync times, and errors."),
        (sync_schedule_set, "Install a daily sync at HH:MM."),
        (sync_schedule_show, "Show current scheduled sync."),
        (sync_schedule_remove, "Uninstall the scheduled sync job."),
    ]:
        register(mcp, fn, fn.__name__, desc)
