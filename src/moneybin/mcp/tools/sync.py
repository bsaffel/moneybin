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

from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
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


def _build_sync_client() -> Any:
    """Construct a SyncClient from current settings."""
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.connectors.sync_client import SyncClient  # noqa: PLC0415

    settings = get_settings()
    if settings.sync.server_url is None:
        raise ValueError(
            "sync.server_url is not configured. "
            "Set MONEYBIN_SYNC__SERVER_URL in your environment."
        )
    return SyncClient(server_url=str(settings.sync.server_url))


@contextmanager
def _build_sync_service() -> Generator[Any, None, None]:
    """Context manager yielding a SyncService with active Database connection."""
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.loaders.plaid_loader import PlaidLoader  # noqa: PLC0415
    from moneybin.services.sync_service import SyncService  # noqa: PLC0415

    client = _build_sync_client()
    with get_database(read_only=False) as db:
        loader = PlaidLoader(db)
        yield SyncService(client=client, db=db, loader=loader)


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False, open_world=True)
def sync_pull(
    institution: str | None = None,
    force: bool = False,
    apply_transforms: bool = True,
) -> ResponseEnvelope:
    """Pull transactions, accounts, balances from connected institutions via moneybin-server.

    Amounts in loaded data follow MoneyBin accounting convention: negative = expense,
    positive = income; the Plaid sign flip happens during ingestion. Returns per-institution
    results including error_code for any failed institutions. Mutates raw.plaid_* tables and
    propagates through SQLMesh to core; idempotent on re-run (transactions upsert by
    (transaction_id, provider_item_id)).

    When apply_transforms=True (default) and rows are loaded, SQLMesh transforms run once
    at end-of-pull so core.dim_accounts and friends reflect the new data before returning.
    Pass apply_transforms=False to defer the transform pass (raw rows still land durably;
    derived models become stale until the next transform_apply).
    """
    with _build_sync_service() as service:
        result = service.pull(
            institution=institution,
            force=force,
            apply_transforms=apply_transforms,
        )
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


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False, open_world=True)
def sync_connect(institution: str | None = None) -> ResponseEnvelope:
    """Initiate a bank-connection flow via moneybin-server's Plaid Hosted Link.

    Returns a URL the user opens in their browser to complete the Plaid UI.
    Does NOT wait for completion — after the user confirms they've finished,
    call sync_connect_status with the returned session_id to verify. The
    link_url is a sensitive one-time credential — present it to the user
    but do not include it in logs or summaries.

    Pass `institution` to re-authenticate an existing connection (Plaid update mode).
    """
    client = _build_sync_client()
    provider_item_id: str | None = None
    if institution:
        matches = [
            inst
            for inst in client.list_institutions()
            if inst.institution_name
            and inst.institution_name.lower() == institution.lower()
        ]
        if len(matches) > 1:
            ids = ", ".join(m.provider_item_id for m in matches)
            return build_error_envelope(
                error=UserError(
                    f"multiple connected institutions match '{institution}' ({ids})",
                    code="ambiguous",
                    hint="Run sync_status to identify them; the duplicate name "
                    "must be disambiguated before sync_connect can target one.",
                ),
                actions=["Run sync_status to list connected institutions."],
            )
        if len(matches) == 1:
            provider_item_id = matches[0].provider_item_id
        # else: name doesn't match any existing connection → new-connection flow
        # per design Section 8; let the server's Link flow name the institution.
    initiate = client.initiate_connect(provider_item_id=provider_item_id)
    return build_envelope(
        data={
            "session_id": initiate.session_id,
            "link_url": initiate.link_url,
            "expiration": initiate.expiration.isoformat(),
        },
        sensitivity="medium",
        actions=[
            "Present link_url to the user and ask them to complete the connection in their browser.",
            "After they confirm completion, call sync_connect_status with session_id to verify.",
            "Once verified, call sync_pull to fetch transactions.",
            "Session expires at the expiration timestamp — beyond that, start a new connect flow.",
        ],
    )


@mcp_tool(sensitivity="low")
def sync_connect_status(session_id: str) -> ResponseEnvelope:
    """Check whether a bank-connection session has completed.

    Call after the user indicates they've finished the Plaid Link flow in their browser.
    Returns connected, pending, or failed. Does NOT loop internally — the agent should
    invoke this when the user signals completion, not poll repeatedly.
    """
    client = _build_sync_client()
    status = client.get_connect_status(session_id)
    actions: list[str] = []
    if status.status == "pending":
        actions = [
            "Connection has not completed yet. Ask the user to finish the flow in their browser, or wait and check again.",
            "If the session expiration has passed, start a new connect flow with sync_connect.",
        ]
    elif status.status == "connected":
        actions = ["Run sync_pull to fetch transactions from the new institution."]
    elif status.status == "failed":
        actions = ["Run sync_connect to retry the connection."]
    return build_envelope(
        data={
            "session_id": status.session_id,
            "status": status.status,
            "provider_item_id": status.provider_item_id,
            "institution_name": status.institution_name,
            "error": status.error,
            "expiration": status.expiration.isoformat(),
        },
        sensitivity="low",
        actions=actions,
    )


@mcp_tool(sensitivity="medium", read_only=False, open_world=True)
def sync_disconnect(institution: str) -> ResponseEnvelope:
    """Remove a bank connection on moneybin-server. Permanent — no revert path.

    Local pulled transactions are preserved in raw.plaid_* and core.fct_transactions;
    the institution simply stops appearing in sync_status and can no longer be
    sync_pull'd. No local app.* state is mutated — connection state lives on the
    server per design Section 4.
    """
    with _build_sync_service() as service:
        service.disconnect(institution=institution)
    return build_envelope(
        data={"status": "disconnected", "institution": institution},
        sensitivity="medium",
        actions=[],
    )


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


SYNC_REVIEW_PROMPT = """\
Review my MoneyBin sync state and flag anything that needs attention.

Use these tools (in order):
1. sync_status — list connected institutions with last sync time, status, and any error guidance.
2. spending_summary detail=summary — optional, for context on recent transaction volume per institution.

Report concisely (bulleted, single paragraph if everything is healthy):

- **Errors:** any institutions with status='error' and the specific re-auth or reconnect action — quote the exact command from the actions hint.
- **Stale data:** any institution whose last_sync is more than 7 days ago, even if status='active'. Suggest running `moneybin sync pull`.
- **Anomalies:** institutions whose recent sync transaction counts are dramatically lower than typical volume (use spending_summary as a rough yardstick — a checking account that's been returning ~30/week suddenly returning 0 is worth flagging).
- **Recommended next action:** one specific command, or "no action needed."

Do not include account numbers, balances, individual transaction descriptions, or merchant names. Stick to counts, dates, status codes, and institution names.
"""


def register_sync_prompts(mcp: FastMCP) -> None:
    """Register sync-related FastMCP prompts."""

    @mcp.prompt(
        name="sync_review", description="Review sync health and suggest next steps."
    )
    def _sync_review() -> str:  # type: ignore[reportUnusedFunction]
        return SYNC_REVIEW_PROMPT


def register_sync_tools(mcp: FastMCP) -> None:
    """Register all sync namespace tools with the FastMCP server."""
    for fn, desc in [
        (
            sync_connect,
            "Initiate a bank-connection flow — returns a URL the user opens in their browser. link_url is a sensitive one-time credential.",
        ),
        (sync_connect_status, "Check whether a connect session has completed."),
        (sync_disconnect, "Remove a bank connection."),
        (
            sync_pull,
            "Pull transactions, accounts, and balances. Amounts use MoneyBin convention (negative = expense).",
        ),
        (sync_status, "Connected institutions, last-sync times, and errors."),
        (sync_schedule_set, "Install a daily sync at HH:MM."),
        (sync_schedule_show, "Show current scheduled sync."),
        (sync_schedule_remove, "Uninstall the scheduled sync job."),
    ]:
        register(mcp, fn, fn.__name__, desc)
