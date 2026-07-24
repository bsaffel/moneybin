"""Inbox MCP tools — drain and preview the watched import folder."""

from __future__ import annotations

import logging

from moneybin.database import get_database
from moneybin.privacy.payloads.imports import (
    ImportInboxPendingPayload,
    ImportInboxSyncPayload,
)
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


def _tier_of(pending_entry: dict[str, object]) -> str:
    """Extract the confidence tier from one pending-entry dict.

    Defaults to ``"low"`` if absent so an unparseable entry is treated as
    needing a mapping override rather than blindly suggesting --accept.
    """
    tier = pending_entry.get("tier")
    return tier if isinstance(tier, str) else "low"


def import_inbox_sync(refresh: bool = True) -> ResponseEnvelope[ImportInboxSyncPayload]:
    """Drain the active profile's import inbox.

    Args:
        refresh: When True (default), run the post-load refresh pipeline
            (matching + SQLMesh apply + categorization) once after all files
            have been imported. Set to False to defer — useful when chaining
            several writes before invoking ``refresh_run`` or refresh
            explicitly.
    """
    from moneybin.config import get_settings

    with get_database(read_only=False) as db:
        service = InboxService(db=db, settings=get_settings())
        sync_result = service.sync(refresh=refresh)

    actions: list[str] = ["Use transactions.search to view newly imported transactions"]
    account_pending = [
        p for p in sync_result.pending if p.get("reason") == "account_confirmation"
    ]
    if account_pending:
        actions.insert(
            0,
            "Some pending files need an account identity — run `moneybin import "
            "confirm <pending-path> --accept --account-binding "
            "<source_key>=<account_id|new>` (--accept ratifies the settled "
            "mapping; source_key is in the .pending.yml sidecar's "
            "account_proposals), or move the file into inbox/<account-slug>/ and "
            "re-run import_inbox_sync",
        )
    # Mapping confirmations only — account_confirmation entries are handled
    # above and take --accept plus --account-binding (not a --mapping override).
    mapping_pending = [
        p for p in sync_result.pending if p.get("reason") != "account_confirmation"
    ]
    if mapping_pending:
        # Tier-aware action: --accept is only meaningful when at least one
        # pending file is non-low (resolve_or_confirm refuses Accept at the
        # low-tier gate, so a blanket --accept hint would loop indefinitely
        # for low-tier-only batches). Each .pending.yml sidecar carries
        # tier-correct per-file recovery hints regardless.
        has_non_low_pending = any(_tier_of(entry) != "low" for entry in mapping_pending)
        if has_non_low_pending:
            actions.insert(
                0,
                "Files in pending/ require confirmation — use `moneybin import "
                "confirm <pending-path> --accept` (or `--mapping field=column`) "
                "per entry; see the .pending.yml sidecars for the detector "
                "proposal and recovery hints",
            )
        else:
            actions.insert(
                0,
                "Files in pending/ require confirmation — only `--mapping "
                "field=column` is usable (every pending file has low-confidence "
                "detection; --accept would be rejected). See the .pending.yml "
                "sidecars for the detector proposal and recovery hints",
            )
    if sync_result.failed:
        actions.insert(
            0,
            "Some files failed — see each .error.yml sidecar's `suggestion` "
            "field for the recovery step",
        )

    threshold = get_settings().categorization.assist_offer_threshold
    uncategorized = _uncategorized_count()
    if uncategorized >= threshold:
        actions.append(
            f"{uncategorized} uncategorized transactions — use "
            "transactions_categorize_assist for AI-assisted categorization, or "
            "`moneybin transactions categorize export-uncategorized` for the CLI bridge"
        )

    return build_envelope(
        data=ImportInboxSyncPayload(
            processed=sync_result.processed,
            failed=sync_result.failed,
            pending=sync_result.pending,
            skipped=sync_result.skipped,
            ignored=sync_result.ignored,
            transforms_applied=sync_result.transforms_applied,
            transforms_duration_seconds=sync_result.transforms_duration_seconds,
            transforms_error=sync_result.transforms_error,
        ),
        actions=actions,
    )


def import_inbox_pending() -> ResponseEnvelope[ImportInboxPendingPayload]:
    """Preview pending items in the active profile's import inbox."""
    return build_envelope(
        data=read_import_inbox_pending(),
        actions=["Use import_inbox_sync to drain the inbox"],
    )


def read_import_inbox_pending() -> ImportInboxPendingPayload:
    """Return the inbox preview payload without invoking a public tool wrapper."""
    service = InboxService.for_active_profile_no_db()
    list_result = service.enumerate()
    return ImportInboxPendingPayload(
        would_process=list_result.would_process,
        ignored=list_result.ignored,
    )


_LEGACY_INTERNAL_CALLBACKS = (import_inbox_pending,)
