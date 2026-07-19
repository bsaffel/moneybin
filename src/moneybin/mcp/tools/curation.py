"""Curation namespace tools — manual entry, notes, tags, splits, import labels, audit log.

Thin MCP wrappers over ``TransactionService``, ``ImportService``, and
``AuditService``. Every mutation passes through the service layer, which
owns transactional integrity and audit emission. The MCP wrapper only
projects service results into JSON-safe dicts and the standard
``{summary, data, actions}`` envelope.

Sensitivity tiers (per ``.claude/rules/mcp.md``):

- Mutations against transaction-keyed state (notes, tags, splits, manual
  entries) and ``import_labels_set`` are tagged ``medium`` because the
  inputs reference identifiers that — together with prior consent — let
  the caller mutate row-level data. Without ``mcp-data-sharing`` consent
  the privacy middleware degrades responses to aggregates without failing.
- ``system_audit`` is also ``medium`` since audit rows can carry
  before/after row-level deltas. Filters are passed through to the
  service.

Tools are registered behind the ``transactions`` and ``system`` core
namespaces; they are *not* tagged with an extended-domain tag and are
therefore visible by default at session start.
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.imports import ImportLabelsSetPayload
from moneybin.privacy.payloads.system import SystemAuditEventPayload, SystemAuditPayload
from moneybin.privacy.payloads.transactions import (
    ManualBatchEntryResult as ManualBatchEntryResultPayload,
)
from moneybin.privacy.payloads.transactions import (
    ManualBatchPayload,
    NoteDeletePayload,
    NotePayload,
    SplitRow,
    SplitsPayload,
    TagRenamePayload,
    TagsPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.audit_service import AuditService
from moneybin.services.import_service import ImportService
from moneybin.services.transaction_service import Note, Split, TransactionService

logger = logging.getLogger(__name__)

_MANUAL_BATCH_MAX = 100


# ---------- helpers ----------


def _note_payload(note: Note) -> NotePayload:
    return NotePayload(
        note_id=note.note_id,
        transaction_id=note.transaction_id,
        text=note.text,
        author=note.author,
        created_at=note.created_at,
    )


def _split_row(split: Split) -> SplitRow:
    return SplitRow(
        split_id=split.split_id,
        transaction_id=split.transaction_id,
        amount=str(split.amount),
        category=split.category,
        subcategory=split.subcategory,
        note=split.note,
        ord=split.ord,
        created_at=split.created_at,
        created_by=split.created_by,
    )


def _coerce_amount(value: Any, where: str) -> Decimal:
    """Accept Decimal, int, float, or string — return Decimal via str round-trip.

    Pydantic-free path: MCP clients send JSON, so a ``Decimal`` arrives as a
    string. Going through ``str()`` first preserves precision for floats
    that originate as decimals upstream.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float | str):
        try:
            return Decimal(str(value))
        except InvalidOperation as e:
            raise UserError(
                f"{where}: amount {value!r} is not a valid decimal",
                code="invalid_amount",
            ) from e
    raise UserError(
        f"{where}: amount must be a number or string, got {type(value).__name__}",
        code="invalid_amount",
    )


def _prepare_manual_entries(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize MCP-shape entries for ``TransactionService.create_manual_batch``.

    The service requires ``amount`` to be a ``Decimal``; JSON delivers it as a
    number or string. We coerce here so the service-layer validator stays
    Decimal-only and rejects unintended floats from internal callers.
    """
    if not 1 <= len(transactions) <= _MANUAL_BATCH_MAX:
        raise UserError(
            f"batch size must be 1..{_MANUAL_BATCH_MAX}, got {len(transactions)}",
            code="invalid_batch_size",
        )
    out: list[dict[str, Any]] = []
    for idx, entry in enumerate(transactions):
        prepared = dict(entry)
        if "amount" in prepared:
            prepared["amount"] = _coerce_amount(
                prepared["amount"], f"transactions[{idx}]"
            )
        out.append(prepared)
    return out


def _prepare_splits(splits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, s in enumerate(splits):
        prepared = dict(s)
        if "amount" not in prepared:
            raise UserError(
                f"splits[{idx}] missing required 'amount'", code="invalid_input"
            )
        prepared["amount"] = _coerce_amount(prepared["amount"], f"splits[{idx}]")
        out.append(prepared)
    return out


# ---------- tools ----------


@mcp_tool(read_only=False, idempotent=False)
def transactions_create(
    transactions: list[dict[str, Any]],
) -> ResponseEnvelope[ManualBatchPayload]:
    """Create 1..100 manual transactions atomically under one import_id.

    Each entry requires ``account_id``, ``amount`` (Decimal-coerced), a
    ``transaction_date`` (ISO 8601 ``YYYY-MM-DD``), and ``description``.
    Optional fields: ``merchant_name``, ``memo``, ``payment_channel``,
    ``transaction_type``, ``check_number``, ``currency_code``,
    ``category``, ``subcategory``.

    The whole batch shares one ``import_id`` (returned as ``batch_id``).
    Rows are exempt from the matcher, so the predicted ``transaction_id``
    is what the next pipeline pass assigns. Validation runs over the full
    batch before any insert — a single bad row aborts the whole batch.
    """
    prepared = _prepare_manual_entries(transactions)
    with get_database(read_only=False) as db:
        result = TransactionService(db).create_manual_batch(prepared, actor="mcp")
    return build_envelope(
        data=ManualBatchPayload(
            batch_id=result.import_id,
            results=[
                ManualBatchEntryResultPayload(
                    transaction_id=r.transaction_id,
                    source_transaction_id=r.source_transaction_id,
                )
                for r in result.results
            ],
        ),
        actions=[
            "Use transactions to confirm the rows landed",
            "Use refresh_run to materialize them into core.fct_transactions",
        ],
    )


def transactions_notes_add(
    transaction_id: str, text: str
) -> ResponseEnvelope[NotePayload]:
    """Append a note to a transaction. Returns the created note row."""
    with get_database(read_only=False) as db:
        note = TransactionService(db).add_note(transaction_id, text, actor="mcp")
    return build_envelope(data=_note_payload(note))


def transactions_notes_edit(note_id: str, text: str) -> ResponseEnvelope[NotePayload]:
    """Update an existing note's text. Returns the updated row."""
    with get_database(read_only=False) as db:
        note = TransactionService(db).edit_note(note_id, text, actor="mcp")
    return build_envelope(data=_note_payload(note))


def transactions_notes_delete(note_id: str) -> ResponseEnvelope[NoteDeletePayload]:
    """Delete a note by ID. Hard-delete; raises LookupError if the note is gone."""
    with get_database(read_only=False) as db:
        TransactionService(db).delete_note(note_id, actor="mcp")
    return build_envelope(data=NoteDeletePayload(note_id=note_id))


def transactions_tags_set(
    transaction_id: str, tags: list[str]
) -> ResponseEnvelope[TagsPayload]:
    """Declarative target-state for a transaction's tags.

    The service diffs the supplied list against current state and emits one
    ``tag.add`` per added tag and one ``tag.remove`` per removed tag inside
    a single DuckDB transaction. The returned payload is the sorted final
    tag list — the diff itself is captured in the audit log.
    """
    with get_database(read_only=False) as db:
        final = TransactionService(db).set_tags(transaction_id, tags, actor="mcp")
    return build_envelope(data=TagsPayload(transaction_id=transaction_id, tags=final))


def transactions_tags_rename(
    old_tag: str, new_tag: str
) -> ResponseEnvelope[TagRenamePayload]:
    """Rename a tag globally. Emits one parent + N child audit events."""
    with get_database(read_only=False) as db:
        res = TransactionService(db).rename_tag(old_tag, new_tag, actor="mcp")
    return build_envelope(
        data=TagRenamePayload(
            old_tag=old_tag,
            new_tag=new_tag,
            row_count=res.row_count,
            parent_audit_id=res.parent_audit_id,
        )
    )


def transactions_splits_set(
    transaction_id: str, splits: list[dict[str, Any]]
) -> ResponseEnvelope[SplitsPayload]:
    """Declarative replace: clear existing splits then add the new sequence.

    Each split requires ``amount`` (Decimal-coerced); ``category``,
    ``subcategory``, and ``note`` are optional. Order is preserved.
    """
    prepared = _prepare_splits(splits)
    with get_database(read_only=False) as db:
        out = TransactionService(db).set_splits(transaction_id, prepared, actor="mcp")
    return build_envelope(data=SplitsPayload(splits=[_split_row(s) for s in out]))


def import_labels_set(
    import_id: str, labels: list[str]
) -> ResponseEnvelope[ImportLabelsSetPayload]:
    """Declarative target-state for an import's labels.

    Replaces the import's label set and emits one full-row ``import.set`` audit
    row (Invariant 10) capturing the complete before/after labels.
    """
    with get_database(read_only=False) as db:
        final = ImportService(db).set_labels(import_id, labels, actor="mcp")
    return build_envelope(
        data=ImportLabelsSetPayload(import_id=import_id, labels=final)
    )


def system_audit(
    filters: dict[str, Any] | None = None, limit: int = 100
) -> ResponseEnvelope[SystemAuditPayload]:
    """List audit events. Filter by actor, ``action_pattern`` (LIKE), target, or time.

    Equivalent to ``moneybin system audit list``. Supply ``audit_id`` via
    ``filters={"audit_id": ...}`` is **not** supported here — use the
    chain-fetch via ``AuditService.chain_for`` (CLI ``system audit show``)
    for the full child chain of one event.
    """
    f = filters or {}
    with get_database(read_only=True) as db:
        events = AuditService(db).list_events(
            actor=f.get("actor"),
            action_pattern=f.get("action_pattern"),
            target_table=f.get("target_table"),
            target_id=f.get("target_id"),
            from_ts=f.get("from") or f.get("from_ts"),
            to_ts=f.get("to") or f.get("to_ts"),
            limit=limit,
        )
    return build_envelope(
        data=SystemAuditPayload(
            events=[
                SystemAuditEventPayload(
                    audit_id=e.audit_id,
                    occurred_at=e.occurred_at,
                    actor=e.actor,
                    action=e.action,
                    target_schema=e.target_schema,
                    target_table=e.target_table,
                    target_id=e.target_id,
                    before_value=e.before_value,
                    after_value=e.after_value,
                    parent_audit_id=e.parent_audit_id,
                    operation_id=e.operation_id,
                    context_json=e.context_json,
                    is_undo=e.is_undo,
                    undoes_operation_id=e.undoes_operation_id,
                )
                for e in events
            ]
        ),
        actions=[
            "Filter with action_pattern='tag.%' / 'note.%' / 'split.%' to drill in",
        ],
    )


# ---------- registration ----------


def register_curation_tools(mcp: FastMCP) -> None:
    """Register the standard manual transaction creation boundary."""
    register(
        mcp,
        transactions_create,
        "transactions_create",
        "Create manual transactions as a validated batch. Amounts use the "
        "accounting convention: negative = expense, positive = income; "
        "transfers exempt. Reverse with system_audit_undo(operation_id).",
    )
