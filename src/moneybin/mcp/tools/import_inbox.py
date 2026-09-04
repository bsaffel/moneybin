"""Inbox MCP tools — drain and preview the watched import folder."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import cast

from moneybin.adapters.refresh_adapters import (
    refresh_rate_gap_hints,
    refresh_step_actions,
)
from moneybin.adapters.rematch_report import retired_transfers_action
from moneybin.database import get_database
from moneybin.privacy.payloads.imports import (
    ImportInboxPendingEntry,
    ImportInboxPendingPayload,
    ImportInboxProcessedEntry,
    ImportInboxSyncPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization.queries import CategorizationQueries
from moneybin.services.inbox_service import InboxService
from moneybin.services.refresh_outcome import refresh_steps_fields

logger = logging.getLogger(__name__)


def _uncategorized_count() -> int:
    """Return the size of the canonical uncategorized queue.

    Reads through ``CategorizationQueries`` so the hint quotes the same N the
    review queue it points at will show. Returns 0 on any error so a DB hiccup
    never breaks the import summary.
    """
    try:
        with get_database(read_only=True) as db:
            return CategorizationQueries(db).count_uncategorized()
    except Exception:  # noqa: BLE001 — never surface DB errors in summary hint
        return 0


def _minted_count(processed: Sequence[Mapping[str, object]]) -> int:
    """Count accounts the drain minted across every imported file.

    The rows arrive as service-shaped ``dict[str, object]``, so the list is
    checked rather than assumed: a malformed entry must undercount, never raise
    on the drain's own success path.

    """
    total = 0
    for entry in processed:
        created = entry.get("accounts_created")
        if isinstance(created, list):
            total += len(created)  # pyright: ignore[reportUnknownArgumentType]
    return total


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

    # `transactions`, not `transactions.search` — the latter is not a registered
    # tool and never was, so an agent following it called nothing.
    actions: list[str] = ["Use transactions to view newly imported transactions"]
    # The drain mints accounts under exactly the conditions import_files does,
    # and the same "gate the merge, not the mint" bargain applies: no confirm, so
    # the surface has to name what it created. Same helper as import_files —
    # unattended drain is where an unannounced account is least likely to be
    # noticed, not where a weaker hint is acceptable.
    from moneybin.mcp.tools.import_tools import accounts_created_action  # noqa: PLC0415

    if minted_action := accounts_created_action(_minted_count(sync_result.processed)):
        actions.insert(0, minted_action)
    # Same reasoning as the minted-accounts hint above, one step stronger: the
    # drain's closing refresh can reverse a transfer the user accepted, and an
    # unattended surface is where an unannounced reversal goes unnoticed
    # longest. Inserted at the front for that reason.
    if retired := retired_transfers_action(
        sync_result.transfers_retired, operation="inbox sync"
    ):
        actions.insert(0, retired)
    account_pending = [
        p for p in sync_result.pending if p.get("reason") == "account_confirmation"
    ]
    if account_pending:
        # The subfolder move is offered only when a tabular file is actually
        # waiting on one: `account_name` is tabular-only, so on OFX and PDF the
        # inbox drops the folder hint and the move returns the file to the same
        # gate. Advertising it there costs an agent a full drain to disprove.
        subfolder_alternative = (
            ", or move the file into inbox/<account-slug>/ and re-run import_inbox_sync"
            if any(p.get("channel") == "tabular" for p in account_pending)
            else ""
        )
        actions.insert(
            0,
            # @N, not source_key: this envelope masks source_account_key (on OFX
            # it is the institution's own <ACCTID>), so the ref is the only
            # referent an agent reading this response can act on.
            "Some pending files need an account identity — run `moneybin import "
            "confirm <pending-path> --accept --account-binding "
            "@N=<account_id|new>` (--accept ratifies the settled mapping; @N is "
            "the proposal_ref on each data.pending[].account_proposals[] entry)"
            f"{subfolder_alternative}",
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

    actions.extend(refresh_rate_gap_hints(sync_result.refresh_steps))

    threshold = get_settings().categorization.assist_offer_threshold
    uncategorized = _uncategorized_count()
    if uncategorized >= threshold:
        actions.append(
            f"{uncategorized} uncategorized transactions — use "
            "transactions_categorize_assist for AI-assisted categorization, or "
            "`moneybin transactions categorize export-uncategorized` for the CLI bridge"
        )

    return build_envelope(
        # The service returns loosely-typed per-file dicts; declaring their shape
        # is this adapter's job, and it is what lets the redaction walk reach
        # `display_name` (USER_NOTE) and `source_account_key`
        # (ACCOUNT_IDENTIFIER) instead of classing each row as a single opaque
        # value. cast, not copy: the shapes already match key for key.
        data=ImportInboxSyncPayload(
            processed=cast(
                "list[ImportInboxProcessedEntry]",
                sync_result.processed,
            ),
            failed=sync_result.failed,
            pending=cast("list[ImportInboxPendingEntry]", sync_result.pending),
            skipped=sync_result.skipped,
            ignored=sync_result.ignored,
            transforms_applied=sync_result.transforms_applied,
            transforms_duration_seconds=sync_result.transforms_duration_seconds,
            transforms_error=sync_result.transforms_error,
            transfers_retired=sync_result.transfers_retired,
            **refresh_steps_fields(sync_result.refresh_steps),
        ),
        actions=actions,
        # `or None` to omit the key when empty, matching `refresh_envelope`.
        recovery_actions=refresh_step_actions(
            sync_result.refresh_steps,
            apply_failed=sync_result.transforms_error is not None,
        )
        or None,
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
