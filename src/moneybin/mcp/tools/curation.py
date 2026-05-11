"""Curation namespace tools — manual entry, notes, tags, splits, import labels, audit log.

Thin MCP wrappers over ``TransactionService``, ``ImportService``, and
``AuditService``. Every mutation passes through the service layer, which
owns transactional integrity and audit emission. The MCP wrapper only
projects service results into JSON-safe dicts and the standard
``{summary, data, actions}`` envelope.

Sensitivity tiers (per ``.claude/rules/mcp-server.md``):

- Mutations against transaction-keyed state (notes, tags, splits, manual
  entries) and ``import_labels_set`` are tagged ``medium`` because the
  inputs reference identifiers that — together with prior consent — let
  the caller mutate row-level data. Without ``mcp-data-sharing`` consent
  the privacy middleware degrades responses to aggregates without failing.
- ``system_audit_list`` is also ``medium`` since audit rows can carry
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
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.audit_service import AuditService
from moneybin.services.import_service import ImportService
from moneybin.services.transaction_service import (
    Note,
    Split,
    TransactionService,
)

logger = logging.getLogger(__name__)

_MANUAL_BATCH_MAX = 100


# ---------- helpers ----------


def _note_dict(note: Note) -> dict[str, Any]:
    return {
        "note_id": note.note_id,
        "transaction_id": note.transaction_id,
        "text": note.text,
        "author": note.author,
        "created_at": note.created_at,
    }


def _split_dict(split: Split) -> dict[str, Any]:
    return {
        "split_id": split.split_id,
        "transaction_id": split.transaction_id,
        "amount": str(split.amount),
        "category": split.category,
        "subcategory": split.subcategory,
        "note": split.note,
        "ord": split.ord,
        "created_at": split.created_at,
        "created_by": split.created_by,
    }


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


def _prepare_manual_entries(
    transactions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
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
                f"splits[{idx}] missing required 'amount'",
                code="invalid_input",
            )
        prepared["amount"] = _coerce_amount(prepared["amount"], f"splits[{idx}]")
        out.append(prepared)
    return out


# ---------- tools ----------


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False)
def transactions_create(
    transactions: list[dict[str, Any]],
) -> ResponseEnvelope:
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
    service = TransactionService(get_database())
    result = service.create_manual_batch(prepared, actor="mcp")
    return build_envelope(
        data={
            "batch_id": result.import_id,
            "results": [
                {
                    "transaction_id": r.transaction_id,
                    "source_transaction_id": r.source_transaction_id,
                }
                for r in result.results
            ],
        },
        sensitivity="medium",
        actions=[
            "Use transactions_search to confirm the rows landed",
            "Use transform_apply to materialize them into core.fct_transactions",
        ],
    )


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False)
def transactions_notes_add(transaction_id: str, text: str) -> ResponseEnvelope:
    """Append a note to a transaction. Returns the created note row."""
    note = TransactionService(get_database()).add_note(
        transaction_id, text, actor="mcp"
    )
    return build_envelope(data=_note_dict(note), sensitivity="medium")


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False)
def transactions_notes_edit(note_id: str, text: str) -> ResponseEnvelope:
    """Update an existing note's text. Returns the updated row."""
    note = TransactionService(get_database()).edit_note(note_id, text, actor="mcp")
    return build_envelope(data=_note_dict(note), sensitivity="medium")


@mcp_tool(
    sensitivity="medium",
    read_only=False,
    destructive=True,
    idempotent=False,
)
def transactions_notes_delete(note_id: str) -> ResponseEnvelope:
    """Delete a note by ID. Hard-delete; raises LookupError if the note is gone."""
    TransactionService(get_database()).delete_note(note_id, actor="mcp")
    return build_envelope(data={"note_id": note_id}, sensitivity="low")


@mcp_tool(sensitivity="medium", read_only=False)
def transactions_tags_set(transaction_id: str, tags: list[str]) -> ResponseEnvelope:
    """Declarative target-state for a transaction's tags.

    The service diffs the supplied list against current state and emits one
    ``tag.add`` per added tag and one ``tag.remove`` per removed tag inside
    a single DuckDB transaction. The returned payload is the sorted final
    tag list — the diff itself is captured in the audit log.
    """
    final = TransactionService(get_database()).set_tags(
        transaction_id, tags, actor="mcp"
    )
    return build_envelope(
        data={"transaction_id": transaction_id, "tags": final},
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium", read_only=False)
def transactions_tags_rename(old_tag: str, new_tag: str) -> ResponseEnvelope:
    """Rename a tag globally. Emits one parent + N child audit events."""
    res = TransactionService(get_database()).rename_tag(old_tag, new_tag, actor="mcp")
    return build_envelope(
        data={"row_count": res.row_count, "parent_audit_id": res.parent_audit_id},
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium", read_only=False)
def transactions_splits_set(
    transaction_id: str, splits: list[dict[str, Any]]
) -> ResponseEnvelope:
    """Declarative replace: clear existing splits then add the new sequence.

    Each split requires ``amount`` (Decimal-coerced); ``category``,
    ``subcategory``, and ``note`` are optional. Order is preserved.
    """
    prepared = _prepare_splits(splits)
    out = TransactionService(get_database()).set_splits(
        transaction_id, prepared, actor="mcp"
    )
    return build_envelope(
        data=[_split_dict(s) for s in out],
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium", read_only=False)
def import_labels_set(import_id: str, labels: list[str]) -> ResponseEnvelope:
    """Declarative target-state for an import's labels.

    Computes the add/remove diff against the prior labels and emits one
    ``import_label.add`` / ``import_label.remove`` per changed entry.
    """
    final = ImportService(get_database()).set_labels(import_id, labels, actor="mcp")
    return build_envelope(
        data={"import_id": import_id, "labels": final},
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def system_audit_list(
    filters: dict[str, Any] | None = None, limit: int = 100
) -> ResponseEnvelope:
    """List audit events. Filter by actor, ``action_pattern`` (LIKE), target, or time.

    Equivalent to ``moneybin system audit list``. Supply ``audit_id`` via
    ``filters={"audit_id": ...}`` is **not** supported here — use the
    chain-fetch via ``AuditService.chain_for`` (CLI ``system audit show``)
    for the full child chain of one event.
    """
    f = filters or {}
    events = AuditService(get_database()).list_events(
        actor=f.get("actor"),
        action_pattern=f.get("action_pattern"),
        target_table=f.get("target_table"),
        target_id=f.get("target_id"),
        from_ts=f.get("from") or f.get("from_ts"),
        to_ts=f.get("to") or f.get("to_ts"),
        limit=limit,
    )
    return build_envelope(
        data=[e.to_dict() for e in events],
        sensitivity="medium",
        actions=[
            "Filter with action_pattern='tag.%' / 'note.%' / 'split.%' to drill in",
        ],
    )


# ---------- registration ----------


def register_curation_tools(mcp: FastMCP) -> None:
    """Register all curation namespace tools with the FastMCP server."""
    register(
        mcp,
        transactions_create,
        "transactions_create",
        "Create 1..100 manual transactions atomically under one import_id. "
        "Amounts use the accounting convention: negative = expense, positive = income; transfers exempt. "
        "Amounts are in the currency named by `summary.display_currency`. "
        "Writes raw.manual_transactions; revert with `moneybin import revert <import_id>`.",
    )
    register(
        mcp,
        transactions_notes_add,
        "transactions_notes_add",
        "Append a note to a transaction. Writes app.transaction_notes; "
        "revert with transactions_notes_delete (hard-delete).",
    )
    register(
        mcp,
        transactions_notes_edit,
        "transactions_notes_edit",
        "Update an existing note's text. Writes app.transaction_notes; "
        "every edit is captured in app.audit_log.",
    )
    register(
        mcp,
        transactions_notes_delete,
        "transactions_notes_delete",
        "Delete a note by ID. Hard-deletes from app.transaction_notes — permanent, "
        "no revert; re-create with transactions_notes_add.",
    )
    register(
        mcp,
        transactions_tags_set,
        "transactions_tags_set",
        "Declarative target-state: replace a transaction's tag set; "
        "emits per-tag audit events for the diff. Writes app.transaction_tags; "
        "revert by calling again with the prior tag list (diff captured in app.audit_log).",
    )
    register(
        mcp,
        transactions_tags_rename,
        "transactions_tags_rename",
        "Rename a tag globally; emits one parent + per-row child audit events. "
        "Writes app.transaction_tags; revert by calling with old_tag and new_tag swapped.",
    )
    register(
        mcp,
        transactions_splits_set,
        "transactions_splits_set",
        "Declarative replace: set a transaction's splits; emits clear + per-split events. "
        "Amounts are in the currency named by `summary.display_currency`. "
        "Writes app.transaction_splits; revert by calling again with the prior split sequence.",
    )
    register(
        mcp,
        import_labels_set,
        "import_labels_set",
        "Declarative target-state: replace an import's labels; "
        "emits per-label add/remove audit events. Writes app.imports.labels; "
        "revert by calling again with the prior label list (diff captured in app.audit_log).",
    )
    register(
        mcp,
        system_audit_list,
        "system_audit_list",
        "List audit events with filters (actor, action_pattern, target, time, limit).",
    )
