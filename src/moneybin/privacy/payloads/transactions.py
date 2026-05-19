"""Typed payload dataclasses for the transactions and curation surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Payloads with ``ACCOUNT_IDENTIFIER`` (CRITICAL) resolve to ``Tier.CRITICAL``:
  - ``TransactionRow`` (account_id always present)
  - ``ManualBatchEntryResult`` (account_id not surfaced directly; batch level
    carries no account_id, so ``ManualBatchPayload`` resolves to ``Tier.LOW``)
  - ``TagsPayload`` (transaction_id only — RECORD_ID → LOW)

Payloads with ``TXN_AMOUNT`` (HIGH) as strongest class:
  - ``SplitPayload``, ``SplitsPayload`` (amount present)

Payloads with ``USER_NOTE`` (MEDIUM) as strongest:
  - ``NotePayload``, ``NoteDeletePayload``, ``TagRenamePayload``
  - ``ReviewStatusPayload`` (aggregate counts only — LOW)

``TransactionGetPayload`` contains ``TransactionRow`` which has ``account_id``
(ACCOUNT_IDENTIFIER → CRITICAL).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass

# ---------------------------------------------------------------------------
# transactions_get
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TransactionRow:
    """One row from core.fct_transactions (transactions_get result)."""

    transaction_id: Annotated[str, DataClass.RECORD_ID]
    # CRITICAL — drives TransactionGetPayload to Tier.CRITICAL
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    transaction_date: Annotated[str, DataClass.TXN_DATE]
    amount: Annotated[Decimal, DataClass.TXN_AMOUNT]
    description: Annotated[str, DataClass.DESCRIPTION]
    memo: Annotated[str | None, DataClass.DESCRIPTION]
    source_type: Annotated[str, DataClass.TXN_TYPE]
    category: Annotated[str | None, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    # notes / tags / splits are nested structures; annotate as USER_NOTE /
    # DESCRIPTION — they carry user-authored free text.
    notes: Annotated[list[dict[str, object]] | None, DataClass.USER_NOTE]
    tags: Annotated[list[str] | None, DataClass.USER_NOTE]
    splits: Annotated[list[dict[str, object]] | None, DataClass.TXN_AMOUNT]


@dataclass(frozen=True, slots=True)
class TransactionGetPayload:
    """Payload for transactions_get."""

    transactions: list[TransactionRow]
    next_cursor: Annotated[str | None, DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# transactions_review
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReviewStatusPayload:
    """Payload for transactions_review — aggregate queue counts only."""

    matches_pending: Annotated[int, DataClass.AGGREGATE]
    categorize_pending: Annotated[int, DataClass.AGGREGATE]
    total: Annotated[int, DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# transactions_create
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ManualBatchEntryResult:
    """One row in the transactions_create result list."""

    transaction_id: Annotated[str, DataClass.RECORD_ID]
    source_transaction_id: Annotated[str, DataClass.RECORD_ID]


@dataclass(frozen=True, slots=True)
class ManualBatchPayload:
    """Payload for transactions_create."""

    batch_id: Annotated[str, DataClass.RECORD_ID]
    results: list[ManualBatchEntryResult]


# ---------------------------------------------------------------------------
# transactions_notes_add / transactions_notes_edit
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NotePayload:
    """Payload for transactions_notes_add and transactions_notes_edit."""

    note_id: Annotated[str, DataClass.RECORD_ID]
    transaction_id: Annotated[str, DataClass.RECORD_ID]
    # USER_NOTE — drives NotePayload to Tier.MEDIUM
    text: Annotated[str, DataClass.USER_NOTE]
    author: Annotated[str, DataClass.TXN_TYPE]
    created_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]


# ---------------------------------------------------------------------------
# transactions_notes_delete
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NoteDeletePayload:
    """Payload for transactions_notes_delete."""

    note_id: Annotated[str, DataClass.RECORD_ID]


# ---------------------------------------------------------------------------
# transactions_tags_set
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TagsPayload:
    """Payload for transactions_tags_set — final sorted tag list."""

    transaction_id: Annotated[str, DataClass.RECORD_ID]
    # USER_NOTE — user-authored free text slugs
    tags: Annotated[list[str], DataClass.USER_NOTE]


# ---------------------------------------------------------------------------
# transactions_tags_rename
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TagRenamePayload:
    """Payload for transactions_tags_rename."""

    row_count: Annotated[int, DataClass.AGGREGATE]
    parent_audit_id: Annotated[str, DataClass.RECORD_ID]


# ---------------------------------------------------------------------------
# transactions_splits_set
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SplitRow:
    """One row of app.transaction_splits in the transactions_splits_set result."""

    split_id: Annotated[str, DataClass.RECORD_ID]
    transaction_id: Annotated[str, DataClass.RECORD_ID]
    # HIGH — drives SplitsPayload to Tier.HIGH
    amount: Annotated[str, DataClass.TXN_AMOUNT]
    category: Annotated[str | None, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    note: Annotated[str | None, DataClass.USER_NOTE]
    ord: Annotated[int, DataClass.AGGREGATE]
    created_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    created_by: Annotated[str, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class SplitsPayload:
    """Payload for transactions_splits_set — ordered list of split rows."""

    splits: list[SplitRow]
