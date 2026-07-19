# src/moneybin/services/transaction_service.py
"""Transaction search service.

Business logic for transaction search and filtering.
Consumed by both MCP tools and CLI commands.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Protocol

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.mcp.write_contracts import (
    AnnotationRequest,
    NoteSet,
    SplitsSet,
    TagRename,
    TagsSet,
)
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.repositories.transaction_splits_repo import TransactionSplitsRepo
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
from moneybin.services._validators import validate_note_text, validate_slug
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization._shared import resolve_category_id
from moneybin.services.mutation_context import operation
from moneybin.tables import (
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    MANUAL_TRANSACTIONS,
)

logger = logging.getLogger(__name__)

# Audit target prefixes (schema, table) for the audit events still emitted
# directly by this service: the cross-row tag.rename parent marker and manual
# entry (raw.*). Notes/tags/splits row mutations go through their repos.
_AUDIT_TARGET_TAGS = ("app", "transaction_tags")
_AUDIT_TARGET_MANUAL = ("raw", "manual_transactions")
_MANUAL_BATCH_MAX = 100
_MANUAL_FORMAT_NAME = "manual_entry"
_MANUAL_SOURCE_TYPE = "manual"
# raw.manual_transactions.source_origin is always 'user' (schema DEFAULT) and is
# the manual native account key's scope; both feed the transaction_id hash.
_MANUAL_SOURCE_ORIGIN = "user"
_MIN_ACCOUNT_FUZZY_CONFIDENCE = 0.4


def _state_digest(value: object) -> str:
    """Hash live preflight state without exposing annotation contents."""
    encoded = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _predict_manual_gold_key(source_transaction_id: str, account_id: str) -> str:
    """Pre-compute the gold ``transaction_id`` the SQLMesh pipeline will assign.

    Mirrors the unmatched-row branch of ``int_transactions__matched`` after the
    ADR-015 / RD-2 re-key, which hashes the immutable source identity (NOT the
    mutable canonical ``account_id``):
    ``SUBSTRING(SHA256(source_type||'|'||source_origin||'|'||source_account_key||'|'||source_transaction_id), 1, 16)``.
    For manual rows ``source_origin='user'`` and ``source_account_key`` is the
    stored ``account_id``. Manual rows are exempt from the matcher (spec Req 6 /
    Task 8) so this branch is the only one they hit. If either side of this hash
    drifts from the SQL, the pre-attached user-category row will silently fail to
    join in ``core.fct_transactions``.
    """
    raw = (
        f"{_MANUAL_SOURCE_TYPE}|{_MANUAL_SOURCE_ORIGIN}|"
        f"{account_id}|{source_transaction_id}"
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class Transaction:
    """Single transaction record."""

    transaction_id: str
    account_id: str
    transaction_date: str
    amount: Decimal
    description: str
    memo: str | None
    source_type: str
    category: str | None
    subcategory: str | None
    notes: list[dict[str, Any]] | None = None
    tags: list[str] | None = None
    splits: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        d: dict[str, Any] = {
            "transaction_id": self.transaction_id,
            "account_id": self.account_id,
            "transaction_date": self.transaction_date,
            "amount": self.amount,
            "description": self.description,
            "source_type": self.source_type,
        }
        if self.memo is not None:
            d["memo"] = self.memo
        if self.category is not None:
            d["category"] = self.category
        if self.subcategory is not None:
            d["subcategory"] = self.subcategory
        if self.notes is not None:
            d["notes"] = self.notes
        if self.tags is not None:
            d["tags"] = self.tags
        if self.splits is not None:
            d["splits"] = self.splits
        return d


@dataclass(slots=True)
class TransactionGetResult:
    """Result of TransactionService.get()."""

    transactions: list[Transaction]
    next_cursor: str | None


@dataclass(slots=True)
class OperationalTransactionResult:
    """Resolved operational query page with an exact filtered total."""

    transactions: list[Transaction]
    total_count: int


@dataclass(frozen=True, slots=True)
class ManualEntryRawResult:
    """Raw-write outcome for a single manual entry.

    ``transaction_id`` is the predicted gold key the SQLMesh pipeline will
    assign on its next pass — manual rows are exempt from the matcher (Task 8)
    so they always fall to the SHA256 fallback in
    ``int_transactions__matched``. Pre-computing it here lets us attach a
    user-category row keyed on the future gold id BEFORE the pipeline runs.
    """

    source_transaction_id: str
    transaction_id: str


@dataclass(frozen=True, slots=True)
class ManualBatchResult:
    """Outcome of one ``create_manual_batch`` call: import_id + ordered rows."""

    import_id: str
    results: list[ManualEntryRawResult]


@dataclass(frozen=True, slots=True)
class TagRenameResult:
    """Result of ``rename_tag``: the parent audit_id and how many rows shifted."""

    parent_audit_id: str
    row_count: int


@dataclass(frozen=True, slots=True)
class AnnotationOutcome:
    """One annotation request's material outcome."""

    kind: Literal["note_set", "tags_set", "splits_set", "tag_rename"]
    target_ids: tuple[str, ...]
    changed: bool


@dataclass(frozen=True, slots=True)
class AnnotationBatchResult:
    """Ordered outcomes from one atomic annotation batch."""

    operation_id: str
    outcomes: tuple[AnnotationOutcome, ...]


@dataclass(frozen=True, slots=True)
class _PreparedTagsSet:
    """Resolved tag target-state diff shared by coarse and granular writes."""

    transaction_id: str
    desired: tuple[str, ...]
    to_add: tuple[str, ...]
    to_remove: tuple[str, ...]

    @property
    def changed(self) -> bool:
        return bool(self.to_add or self.to_remove)

    @property
    def destructive(self) -> bool:
        return bool(self.to_remove)


@dataclass(frozen=True, slots=True)
class _PreparedSplit:
    """One validated split with its canonical category identity."""

    amount: Decimal
    category: str | None
    subcategory: str | None
    category_id: str | None
    note: str | None


class _SplitTargetLike(Protocol):
    """Common shape accepted by the shared split preparation engine."""

    @property
    def amount(self) -> Decimal: ...

    @property
    def category(self) -> str | None: ...

    @property
    def subcategory(self) -> str | None: ...

    @property
    def note(self) -> str | None: ...


@dataclass(frozen=True, slots=True)
class _GranularSplitTarget:
    """Legacy granular adapter input after its original validation."""

    amount: Decimal
    category: Any
    subcategory: Any
    note: Any


@dataclass(frozen=True, slots=True)
class _PreparedSplitsSet:
    """Resolved split target-state diff shared by coarse and granular writes."""

    transaction_id: str
    current: tuple[_PreparedSplit, ...]
    desired: tuple[_PreparedSplit, ...]
    changed: bool
    destructive: bool


@dataclass(frozen=True, slots=True)
class _PreparedTagRename:
    """Resolved global tag rename shared by coarse and granular writes."""

    old_name: str
    new_name: str
    target_ids: tuple[str, ...]

    @property
    def changed(self) -> bool:
        return bool(self.target_ids)


PreparedMutation = _PreparedTagsSet | _PreparedSplitsSet | _PreparedTagRename


@dataclass(frozen=True, slots=True)
class _PreparedAnnotation:
    """A fully resolved annotation request, ready for a single write transaction."""

    request: AnnotationRequest
    target_ids: tuple[str, ...] = ()
    changed: bool = False
    destructive: bool = False
    mutation: PreparedMutation | None = None
    state_digest: str = ""


@dataclass(frozen=True, slots=True)
class AnnotationPlan:
    """Stable preflight snapshot used for confirmation and atomic execution."""

    items: tuple[_PreparedAnnotation, ...]

    @property
    def destructive(self) -> bool:
        """Return whether any changed item removes or replaces live state."""
        return any(item.destructive for item in self.items if item.changed)

    @property
    def changed_count(self) -> int:
        """Return the number of material target-state changes."""
        return sum(item.changed for item in self.items)

    @property
    def resolved_ids(self) -> tuple[str, ...]:
        """Return exact resolved targets and opaque live-state fingerprints."""
        targets = tuple(
            sorted({
                f"{item.request.kind}:{target_id}"
                for item in self.items
                for target_id in item.target_ids
            })
        )
        states = tuple(
            f"state:{index}:{item.state_digest}"
            for index, item in enumerate(self.items)
        )
        return targets + states


@dataclass(frozen=True, slots=True)
class Split:
    """One row of ``app.transaction_splits``."""

    split_id: str
    transaction_id: str
    amount: Decimal
    category: str | None
    subcategory: str | None
    note: str | None
    ord: int
    created_at: str
    created_by: str


@dataclass(frozen=True, slots=True)
class Note:
    """One row of ``app.transaction_notes`` (multi-note shape)."""

    note_id: str
    transaction_id: str
    text: str
    author: str
    created_at: str


class TransactionService:
    """Transaction search, notes, and tag operations.

    Methods return typed dataclasses with a ``to_envelope()`` method.
    """

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Initialize with an open Database; lazily build AuditService if absent.

        ``audit`` is keyword-only so existing positional call sites
        (``TransactionService(db)``) continue to work without modification.
        """
        self._db = db
        self._audit = audit if audit is not None else AuditService(db)
        # Repo-backed mutations for notes/tags/splits (Invariant 10); share the
        # audit service so emissions land on one connection/transaction.
        self._notes_repo = TransactionNotesRepo(db, audit=self._audit)
        self._tags_repo = TransactionTagsRepo(db, audit=self._audit)
        self._splits_repo = TransactionSplitsRepo(db, audit=self._audit)

    def apply_annotations(
        self,
        requests: Sequence[AnnotationRequest],
        *,
        actor: str,
        operation_id: str,
        verify: Callable[[AnnotationPlan], None] | None = None,
    ) -> AnnotationBatchResult:
        """Apply complete annotation target states atomically after full preflight."""
        plan = self.preview_annotations(requests)
        if verify is not None:
            verify(plan)
        with operation(operation_id):
            self._db.begin()
            try:
                outcomes = tuple(
                    self._apply_prepared_annotation(item, actor=actor)
                    for item in plan.items
                )
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        return AnnotationBatchResult(operation_id=operation_id, outcomes=outcomes)

    def preview_annotations(
        self,
        requests: Sequence[AnnotationRequest],
    ) -> AnnotationPlan:
        """Resolve an exact batch diff without mutating state."""
        prepared = tuple(self._prepare_annotation(request) for request in requests)
        self._reject_composed_annotations(prepared)
        plan = AnnotationPlan(items=prepared)
        if plan.changed_count == 0:
            raise UserError(
                "The requested annotation states are already current.",
                code=error_codes.MUTATION_NOTHING_TO_DO,
            )
        return plan

    def _prepare_annotation(self, request: AnnotationRequest) -> _PreparedAnnotation:
        """Resolve every batch target before writes begin."""
        if isinstance(request, NoteSet):
            self._annotation_transaction_amount(request.transaction_id)
            if request.note is not None:
                validate_note_text(request.note)
            desired = [] if request.note is None else [request.note]
            current = self.list_notes(request.transaction_id)
            return _PreparedAnnotation(
                request=request,
                target_ids=(request.transaction_id,),
                changed=[note.text for note in current] != desired,
                destructive=bool(current),
                state_digest=_state_digest([note.text for note in current]),
            )

        if isinstance(request, TagsSet):
            self._annotation_transaction_amount(request.transaction_id)
            mutation = self._prepare_tags_set(request.transaction_id, request.tags)
            return _PreparedAnnotation(
                request=request,
                target_ids=(request.transaction_id,),
                changed=mutation.changed,
                destructive=mutation.destructive,
                mutation=mutation,
                state_digest=_state_digest({
                    "to_add": mutation.to_add,
                    "to_remove": mutation.to_remove,
                }),
            )

        if isinstance(request, SplitsSet):
            transaction_amount = self._annotation_transaction_amount(
                request.transaction_id
            )
            mutation = self._prepare_splits_set(
                request.transaction_id,
                request.splits,
                expected_total=transaction_amount,
                require_categories=True,
            )
            return _PreparedAnnotation(
                request=request,
                target_ids=(request.transaction_id,),
                changed=mutation.changed,
                destructive=mutation.destructive,
                mutation=mutation,
                state_digest=_state_digest({
                    "changed": mutation.changed,
                    "destructive": mutation.destructive,
                    "current": mutation.current,
                    "desired": mutation.desired,
                }),
            )

        mutation = self._prepare_tag_rename(request.old_name, request.new_name)
        return _PreparedAnnotation(
            request=request,
            target_ids=mutation.target_ids,
            changed=mutation.changed,
            destructive=mutation.changed,
            mutation=mutation,
            state_digest=_state_digest(mutation.target_ids),
        )

    def _reject_composed_annotations(
        self,
        prepared: tuple[_PreparedAnnotation, ...],
    ) -> None:
        """Reject batches whose independently resolved diffs alter each other."""
        seen: set[tuple[str, str]] = set()
        earlier_tag_effects: list[tuple[str, str]] = []
        for item in prepared:
            request = item.request
            mutation = item.mutation
            if isinstance(request, TagRename):
                key = (request.kind, f"{request.old_name}:{request.new_name}")
            else:
                key = (request.kind, request.transaction_id)
            if key in seen:
                raise UserError(
                    "Annotation requests overlap the same target state.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            seen.add(key)

            if isinstance(mutation, _PreparedTagRename):
                targets = set(mutation.target_ids)
                if any(
                    tag == mutation.old_name
                    or (tag == mutation.new_name and transaction_id in targets)
                    for transaction_id, tag in earlier_tag_effects
                ):
                    raise UserError(
                        "Annotation requests overlap because an earlier tag "
                        "mutation changes a later prepared rename.",
                        code=error_codes.MUTATION_INVALID_INPUT,
                    )
                earlier_tag_effects.extend(
                    (transaction_id, tag)
                    for transaction_id in mutation.target_ids
                    for tag in (mutation.old_name, mutation.new_name)
                )
                continue

            if isinstance(mutation, _PreparedTagsSet):
                if any(
                    transaction_id == mutation.transaction_id
                    for transaction_id, _tag in earlier_tag_effects
                ):
                    raise UserError(
                        "Annotation requests overlap because an earlier tag "
                        "mutation changes a later prepared tag set.",
                        code=error_codes.MUTATION_INVALID_INPUT,
                    )
                earlier_tag_effects.extend(
                    (mutation.transaction_id, tag)
                    for tag in (*mutation.to_add, *mutation.to_remove)
                )

    def _annotation_transaction_amount(self, transaction_id: str) -> Decimal:
        """Resolve one annotation transaction and return its signed amount."""
        row = self._db.conn.execute(
            f"SELECT amount FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",  # noqa: S608  # TableRef constant
            [transaction_id],
        ).fetchone()
        if row is None:
            raise UserError(
                "The transaction reference did not match a transaction.",
                code="TRANSACTION_REFERENCE_NOT_FOUND",
            )
        amount = row[0]
        return amount if isinstance(amount, Decimal) else Decimal(str(amount))

    def _apply_prepared_annotation(
        self,
        prepared: _PreparedAnnotation,
        *,
        actor: str,
    ) -> AnnotationOutcome:
        """Apply one preflighted target state inside the caller's transaction."""
        request = prepared.request
        if isinstance(request, NoteSet):
            if prepared.changed:
                notes = self.list_notes(request.transaction_id)
                for note in notes:
                    self._notes_repo.delete(
                        note_id=note.note_id,
                        actor=actor,
                        in_outer_txn=True,
                    )
                if request.note is not None:
                    self._notes_repo.add(
                        transaction_id=request.transaction_id,
                        note_id=uuid.uuid4().hex[:12],
                        text=request.note,
                        actor=actor,
                        in_outer_txn=True,
                    )
            return AnnotationOutcome(
                kind=request.kind,
                target_ids=prepared.target_ids,
                changed=prepared.changed,
            )

        if isinstance(request, TagsSet):
            mutation = prepared.mutation
            if not isinstance(mutation, _PreparedTagsSet):
                raise RuntimeError("Prepared tags mutation is missing")
            self._apply_tags_set(mutation, actor=actor, in_outer_txn=True)
            return AnnotationOutcome(
                kind=request.kind,
                target_ids=prepared.target_ids,
                changed=mutation.changed,
            )

        if isinstance(request, SplitsSet):
            mutation = prepared.mutation
            if not isinstance(mutation, _PreparedSplitsSet):
                raise RuntimeError("Prepared splits mutation is missing")
            self._apply_splits_set(mutation, actor=actor, in_outer_txn=True)
            return AnnotationOutcome(
                kind=request.kind,
                target_ids=prepared.target_ids,
                changed=mutation.changed,
            )

        mutation = prepared.mutation
        if not isinstance(mutation, _PreparedTagRename):
            raise RuntimeError("Prepared tag rename mutation is missing")
        self._apply_tag_rename(mutation, actor=actor, in_outer_txn=True)
        return AnnotationOutcome(
            kind=request.kind,
            target_ids=prepared.target_ids,
            changed=mutation.changed,
        )

    def _resolve_account_ids(self, accounts: list[str]) -> list[str]:
        """Resolve display names or IDs to account_id strings.

        Batches exact account_id lookups in one query, then fuzzy-matches any
        remaining entries via AccountService. Unresolvable entries are silently
        skipped.
        """
        from moneybin.services.account_service import AccountService

        placeholders = ", ".join("?" * len(accounts))
        exact_rows = self._db.execute(
            f"SELECT account_id FROM {DIM_ACCOUNTS.full_name} WHERE account_id IN ({placeholders})",  # noqa: S608  # TableRef constant
            accounts,
        ).fetchall()
        exact_ids = {str(r[0]) for r in exact_rows}

        resolved: list[str] = []
        unmatched: list[str] = []
        for a in accounts:
            (resolved if a in exact_ids else unmatched).append(a)

        if unmatched:
            service = AccountService(self._db)
            for entry in unmatched:
                payload = service.resolve(entry, limit=1)
                if (
                    payload.matches
                    and payload.matches[0].confidence >= _MIN_ACCOUNT_FUZZY_CONFIDENCE
                ):
                    resolved.append(payload.matches[0].account_id)
        return resolved

    def get(
        self,
        *,
        accounts: list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        categories: list[str] | None = None,
        amount_min: Decimal | None = None,
        amount_max: Decimal | None = None,
        description: str | None = None,
        uncategorized_only: bool = False,
        limit: int = 50,
        cursor: str | None = None,
    ) -> TransactionGetResult:
        """Fetch transactions with optional filtering and cursor-based pagination.

        Reads from core.fct_transactions, which already joins curation columns
        (notes, tags, splits) from the app schema. Account entries in `accounts`
        are resolved: exact account_id matches are used directly; everything else
        is fuzzy-matched against display names via AccountService. Unresolvable
        entries are silently skipped.

        Pagination is offset-based internally; the cursor is base64(str(offset))
        so callers treat it as opaque.
        """
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        if cursor is not None:
            try:
                offset = int(base64.b64decode(cursor.encode()).decode())
            except (binascii.Error, UnicodeDecodeError, ValueError) as e:
                raise ValueError(f"invalid cursor: {cursor!r}") from e
            if offset < 0:
                raise ValueError(f"invalid cursor (negative offset): {cursor!r}")
        else:
            offset = 0

        account_ids: list[str] | None = None
        if accounts:
            account_ids = self._resolve_account_ids(accounts)
            if not account_ids:
                return TransactionGetResult(transactions=[], next_cursor=None)
        page = self._query_transactions(
            account_ids=account_ids,
            date_from=date_from,
            date_to=date_to,
            categories=categories,
            merchant_id=None,
            amount_min=amount_min,
            amount_max=amount_max,
            text=description,
            uncategorized_only=uncategorized_only,
            limit=limit,
            offset=offset,
        )
        has_more = page.total_count > offset + limit

        next_cursor = (
            base64.b64encode(str(offset + limit).encode()).decode()
            if has_more
            else None
        )

        logger.info(
            f"transactions_get returned {len(page.transactions)} rows "
            f"(offset={offset}, has_more={has_more})"
        )
        return TransactionGetResult(
            transactions=page.transactions,
            next_cursor=next_cursor,
        )

    def query_operational(
        self,
        *,
        account_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        merchant_id: str | None = None,
        category: str | None = None,
        amount_min: Decimal | None = None,
        amount_max: Decimal | None = None,
        text: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> OperationalTransactionResult:
        """Query the cutover operational surface with already-resolved IDs."""
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        if offset < 0:
            raise ValueError(f"offset must be >= 0, got {offset}")
        return self._query_transactions(
            account_ids=[account_id] if account_id is not None else None,
            date_from=date_from,
            date_to=date_to,
            categories=[category] if category is not None else None,
            merchant_id=merchant_id,
            amount_min=amount_min,
            amount_max=amount_max,
            text=text,
            uncategorized_only=False,
            limit=limit,
            offset=offset,
        )

    def _query_transactions(
        self,
        *,
        account_ids: list[str] | None,
        date_from: str | None,
        date_to: str | None,
        categories: list[str] | None,
        merchant_id: str | None,
        amount_min: Decimal | None,
        amount_max: Decimal | None,
        text: str | None,
        uncategorized_only: bool,
        limit: int,
        offset: int,
    ) -> OperationalTransactionResult:
        """Run the shared parameterized transaction filter and page query."""
        conditions: list[str] = []
        params: list[object] = []

        if account_ids:
            placeholders = ", ".join("?" * len(account_ids))
            conditions.append(f"account_id IN ({placeholders})")
            params.extend(account_ids)
        if date_from:
            conditions.append("transaction_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("transaction_date <= ?")
            params.append(date_to)
        if categories:
            placeholders = ", ".join("?" * len(categories))
            conditions.append(f"category IN ({placeholders})")
            params.extend(categories)
        if merchant_id is not None:
            conditions.append("merchant_id = ?")
            params.append(merchant_id)
        if amount_min is not None:
            conditions.append("amount >= ?")
            params.append(amount_min)
        if amount_max is not None:
            conditions.append("amount <= ?")
            params.append(amount_max)
        if text:
            escaped = text.replace("!", "!!").replace("%", "!%").replace("_", "!_")
            conditions.append(
                "(description ILIKE ? ESCAPE '!' OR memo ILIKE ? ESCAPE '!')"
            )
            like = f"%{escaped}%"
            params.extend([like, like])
        if uncategorized_only:
            conditions.append("categorized_by IS NULL")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        total_row = self._db.execute(
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} {where}",  # noqa: S608  # TableRef + fixed predicates
            params,
        ).fetchone()
        total_count = int(total_row[0]) if total_row is not None else 0
        rows = self._db.execute(
            f"""
            SELECT
                transaction_id, account_id, transaction_date, amount,
                description, memo, source_type, category, subcategory,
                notes, tags, splits
            FROM {FCT_TRANSACTIONS.full_name}
            {where}
            ORDER BY transaction_date DESC, transaction_id
            LIMIT ? OFFSET ?
            """,  # noqa: S608  # TableRef + fixed predicates
            [*params, limit, offset],
        ).fetchall()
        return OperationalTransactionResult(
            transactions=[
                Transaction(
                    transaction_id=str(row[0]),
                    account_id=str(row[1]),
                    transaction_date=str(row[2]),
                    amount=Decimal(str(row[3])),
                    description=str(row[4]),
                    memo=str(row[5]) if row[5] else None,
                    source_type=str(row[6]),
                    category=str(row[7]) if row[7] else None,
                    subcategory=str(row[8]) if row[8] else None,
                    notes=[dict(n) for n in row[9]] if row[9] else None,
                    tags=list(row[10]) if row[10] else None,
                    splits=[dict(s) for s in row[11]] if row[11] else None,
                )
                for row in rows
            ],
            total_count=total_count,
        )

    # ------------------------------------------------------------------
    # Manual entry — raw-write half (spec Req 1–6, Task 7a).
    # ------------------------------------------------------------------

    def create_manual_batch(
        self, entries: list[dict[str, Any]], *, actor: str
    ) -> ManualBatchResult:
        """Write a batch of manual transactions to ``raw.manual_transactions``.

        Validates every entry up front (account exists, amount is non-zero
        ``Decimal``, transaction_date is parseable, description non-empty);
        raises ``ValueError`` with the offending index on the first failure
        before opening any transaction. Allocates one ``raw.import_log`` row
        for the batch via ``ImportService.allocate_import_log`` and inserts
        every row under that ``import_id`` inside a single DuckDB transaction
        alongside one ``manual.create`` audit event.

        This is Task 7a: the raw-write path only. The pipeline is **not**
        triggered here — the next normal ``import_file`` / ``transform apply``
        pass picks these rows up.

        Categorization (when an entry carries a non-empty ``category``) runs
        in its own dedicated transaction *after* the raw-write commits. The
        whole categorization batch is one atomic txn — either every supplied
        category lands, or none do. Raw rows always remain on category
        failure; the next pipeline pass picks them up uncategorized.
        """
        if not 1 <= len(entries) <= _MANUAL_BATCH_MAX:
            raise ValueError(
                f"manual batch size must be 1..{_MANUAL_BATCH_MAX}, got {len(entries)}"
            )

        prepared: list[dict[str, Any]] = []
        for idx, raw in enumerate(entries):
            prepared.append(self._validate_manual_entry(raw, idx))

        # Defer the ImportService import — allocate_import_log lives there and
        # services have a soft no-cycle convention; ImportService imports from
        # loaders only, so the local import keeps both directions clean.
        from moneybin.services.import_service import ImportService

        import_id = ImportService(self._db).allocate_import_log(
            source_type="manual",
            format_name=_MANUAL_FORMAT_NAME,
            actor=actor,
        )

        results: list[ManualEntryRawResult] = []
        self._db.begin()
        try:
            for entry in prepared:
                source_transaction_id = "manual_" + uuid.uuid4().hex[:12]
                transaction_id = _predict_manual_gold_key(
                    source_transaction_id, entry["account_id"]
                )
                # Persist the predicted ``transaction_id`` alongside the source
                # id so the doctor ``orphan_app_state`` audit can join on it to
                # suppress false-positives for notes/tags written against this
                # row in the window between ``transactions_create`` and the
                # next ``refresh_run`` (which materializes the row in
                # ``core.fct_transactions``). Migration V026 added the column;
                # the hash here mirrors ``_predict_manual_gold_key`` exactly.
                self._db.conn.execute(
                    f"""
                    INSERT INTO {MANUAL_TRANSACTIONS.full_name} (
                        source_transaction_id, import_id, account_id,
                        transaction_date, amount, description, merchant_name,
                        memo, category, subcategory, payment_channel,
                        transaction_type, check_number, currency_code,
                        created_by, transaction_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        source_transaction_id,
                        import_id,
                        entry["account_id"],
                        entry["transaction_date"],
                        entry["amount"],
                        entry["description"],
                        entry.get("merchant_name"),
                        entry.get("memo"),
                        # The ``category`` / ``subcategory`` columns on
                        # ``raw.manual_transactions`` are intentionally NULL —
                        # user-supplied categories live in
                        # ``app.transaction_categories`` (written below) so
                        # they're treated identically to categories on rows
                        # imported from any other source.
                        None,
                        None,
                        entry.get("payment_channel"),
                        entry.get("transaction_type"),
                        entry.get("check_number"),
                        entry.get("currency_code") or "USD",
                        actor,
                        transaction_id,
                    ],
                )
                results.append(
                    ManualEntryRawResult(
                        source_transaction_id=source_transaction_id,
                        transaction_id=transaction_id,
                    )
                )

            self._audit.record_audit_event(
                action="manual.create",
                target=(*_AUDIT_TARGET_MANUAL, import_id),
                before=None,
                after={"row_count": len(results)},
                actor=actor,
            )
            self._db.commit()
        except Exception:
            # Any failure between allocate_import_log() and the commit leaves
            # an orphaned ``importing``-status row in raw.import_log that
            # blocks re-imports and shows up in `moneybin import history`.
            # Mirror the OFX path: mark the batch as failed before re-raising.
            self._db.rollback()
            from moneybin.loaders import import_log

            import_log.finalize_import(
                self._db,
                import_id,
                status="failed",
                rows_total=0,
                rows_imported=0,
            )
            raise

        # Attach user-supplied categories in one atomic txn AFTER the raw-write
        # commits. All-or-nothing: a failure on entry N rolls back entries
        # 0..N-1's category rows so the caller sees a clean failure rather
        # than partial categorization. The raw rows always remain — the next
        # pipeline pass picks them up uncategorized.
        from moneybin.services.categorization import CategorizationService

        cat_service = CategorizationService(self._db, audit=self._audit)
        cat_entries = [
            (entry, raw_result)
            for entry, raw_result in zip(prepared, results, strict=True)
            if isinstance(entry.get("category"), str) and entry["category"].strip()
        ]
        if cat_entries:
            self._db.begin()
            try:
                for entry, raw_result in cat_entries:
                    cat_service.set_category_in_active_txn(
                        raw_result.transaction_id,
                        category=entry["category"],
                        subcategory=entry.get("subcategory"),
                        categorized_by="user",
                        actor=actor,
                    )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

        logger.info(
            f"manual.create import_id={import_id} row_count={len(results)} "
            f"actor={actor}"
        )
        return ManualBatchResult(import_id=import_id, results=results)

    def _validate_manual_entry(self, entry: dict[str, Any], idx: int) -> dict[str, Any]:
        """Validate one manual-entry dict; raise ``ValueError`` with index hint."""
        account_id = entry.get("account_id")
        if not isinstance(account_id, str) or not account_id:
            raise ValueError(f"entries[{idx}].account_id must be a non-empty string")
        row = self._db.conn.execute(
            f"SELECT 1 FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ?",
            [account_id],
        ).fetchone()
        if row is None:
            raise ValueError(
                f"entries[{idx}].account_id={account_id!r} not found in "
                f"{DIM_ACCOUNTS.full_name}"
            )

        amount = entry.get("amount")
        if not isinstance(amount, Decimal):
            raise ValueError(
                f"entries[{idx}].amount must be Decimal, got {type(amount).__name__}"
            )
        if amount == 0:
            raise ValueError(f"entries[{idx}].amount must be non-zero")

        raw_date = entry.get("transaction_date")
        parsed_date: date
        if isinstance(raw_date, date) and not isinstance(raw_date, datetime):
            parsed_date = raw_date
        elif isinstance(raw_date, datetime):
            parsed_date = raw_date.date()
        elif isinstance(raw_date, str) and raw_date:
            try:
                parsed_date = date.fromisoformat(raw_date)
            except ValueError as e:
                raise ValueError(
                    f"entries[{idx}].transaction_date {raw_date!r} is not "
                    f"ISO 8601 (YYYY-MM-DD)"
                ) from e
        else:
            raise ValueError(
                f"entries[{idx}].transaction_date is required (date or YYYY-MM-DD)"
            )

        description = entry.get("description")
        if not isinstance(description, str) or not description.strip():
            raise ValueError(f"entries[{idx}].description must be a non-empty string")

        return {
            "account_id": account_id,
            "amount": amount,
            "transaction_date": parsed_date,
            "description": description,
            "merchant_name": entry.get("merchant_name"),
            "memo": entry.get("memo"),
            "payment_channel": entry.get("payment_channel"),
            "transaction_type": entry.get("transaction_type"),
            "check_number": entry.get("check_number"),
            "currency_code": entry.get("currency_code"),
            "category": entry.get("category"),
            "subcategory": entry.get("subcategory"),
        }

    # ------------------------------------------------------------------
    # Notes (multi-note threads on a transaction; spec Req 9–12)
    # ------------------------------------------------------------------

    def add_note(self, transaction_id: str, text: str, *, actor: str) -> Note:
        """Append a note to a transaction; emit ``note.add`` audit event.

        Generates a 12-hex truncated UUID4 for ``note_id``. The mutation and
        the audit row land in the same DuckDB transaction so failures roll
        both back together.
        """
        validate_note_text(text)
        note_id = uuid.uuid4().hex[:12]
        self._notes_repo.add(
            transaction_id=transaction_id, note_id=note_id, text=text, actor=actor
        )
        row = self._db.conn.execute(
            """
            SELECT note_id, transaction_id, text, author, created_at
              FROM app.transaction_notes
             WHERE note_id = ?
            """,
            [note_id],
        ).fetchone()
        if row is None:  # defensive — insert just succeeded
            raise RuntimeError(f"note_id={note_id} vanished after insert")
        logger.info(f"note.add note_id={note_id} actor={actor}")
        return _row_to_note(row)

    def edit_note(self, note_id: str, text: str, *, actor: str) -> Note:
        """Update note text; emit ``note.edit`` audit event.

        Raises ``LookupError`` if ``note_id`` is unknown.
        """
        validate_note_text(text)
        self._notes_repo.edit(note_id=note_id, text=text, actor=actor)
        row = self._db.conn.execute(
            """
            SELECT note_id, transaction_id, text, author, created_at
              FROM app.transaction_notes
             WHERE note_id = ?
            """,
            [note_id],
        ).fetchone()
        if row is None:
            raise RuntimeError(f"note_id={note_id} vanished after update")
        logger.info(f"note.edit note_id={note_id} actor={actor}")
        return _row_to_note(row)

    def delete_note(self, note_id: str, *, actor: str) -> None:
        """Delete a note; emit ``note.delete`` audit event with ``after=None``.

        Raises ``LookupError`` if ``note_id`` is unknown.
        """
        self._notes_repo.delete(note_id=note_id, actor=actor)
        logger.info(f"note.delete note_id={note_id} actor={actor}")

    # ------------------------------------------------------------------
    # Tags (slug-flavored labels on a transaction; spec Req 13–16)
    # ------------------------------------------------------------------

    def add_tags(
        self, transaction_id: str, tags: list[str], *, actor: str
    ) -> list[str]:
        """Apply tags to a transaction; emit one ``tag.add`` event per new tag.

        Idempotent: re-adding an existing tag is skipped silently — no row change
        and no audit row (DN2: no ``noop`` audit noise). All tag patterns are
        validated up front so a bad tag never half-mutates state. Returns the
        list of tags that were actually inserted (excludes the skipped ones).
        """
        for t in tags:
            validate_slug(t)
        added: list[str] = []
        self._db.begin()
        try:
            for tag in tags:
                existed = self._db.conn.execute(
                    "SELECT 1 FROM app.transaction_tags "
                    "WHERE transaction_id = ? AND tag = ?",
                    [transaction_id, tag],
                ).fetchone()
                if existed:
                    continue  # idempotent: re-adding an existing tag is a no-op
                self._tags_repo.add(
                    transaction_id=transaction_id,
                    tag=tag,
                    actor=actor,
                    in_outer_txn=True,
                )
                added.append(tag)
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        logger.info(
            f"tag.add transaction_id={transaction_id} added={len(added)} "
            f"requested={len(tags)} actor={actor}"
        )
        return added

    def remove_tags(
        self, transaction_id: str, tags: list[str], *, actor: str
    ) -> list[str]:
        """Remove tags from a transaction; emit one ``tag.remove`` per removed tag.

        Idempotent: removing an absent tag is skipped silently — no row change
        and no audit row (DN2). Returns the list of tags that were actually
        deleted.
        """
        removed: list[str] = []
        self._db.begin()
        try:
            for tag in tags:
                existed = self._db.conn.execute(
                    "SELECT 1 FROM app.transaction_tags "
                    "WHERE transaction_id = ? AND tag = ?",
                    [transaction_id, tag],
                ).fetchone()
                if not existed:
                    continue  # idempotent: removing an absent tag is a no-op
                self._tags_repo.remove(
                    transaction_id=transaction_id,
                    tag=tag,
                    actor=actor,
                    in_outer_txn=True,
                )
                removed.append(tag)
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        logger.info(
            f"tag.remove transaction_id={transaction_id} removed={len(removed)} "
            f"requested={len(tags)} actor={actor}"
        )
        return removed

    def set_tags(
        self, transaction_id: str, tags: list[str], *, actor: str
    ) -> list[str]:
        """Declarative target-state. Diffs current vs desired and writes the delta.

        Validates every tag, then computes additions and deletions and applies
        them atomically in a single DuckDB transaction so the row state and
        all audit events commit (or roll back) together. The MCP-flavored
        counterpart to imperative ``add_tags`` / ``remove_tags``. Returns the
        sorted final tag list.
        """
        prepared = self._prepare_tags_set(transaction_id, tags)
        self._db.begin()
        try:
            self._apply_tags_set(prepared, actor=actor, in_outer_txn=True)
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        logger.info(
            f"tag.set transaction_id={transaction_id} added={len(prepared.to_add)} "
            f"removed={len(prepared.to_remove)} actor={actor}"
        )
        return list(prepared.desired)

    def _prepare_tags_set(
        self,
        transaction_id: str,
        tags: Sequence[str],
    ) -> _PreparedTagsSet:
        """Validate and resolve one declarative tag diff."""
        for tag in tags:
            validate_slug(tag)
        desired = set(tags)
        current = set(self.list_tags(transaction_id))
        return _PreparedTagsSet(
            transaction_id=transaction_id,
            desired=tuple(sorted(desired)),
            to_add=tuple(sorted(desired - current)),
            to_remove=tuple(sorted(current - desired)),
        )

    def _apply_tags_set(
        self,
        prepared: _PreparedTagsSet,
        *,
        actor: str,
        in_outer_txn: bool,
    ) -> None:
        """Apply one prepared tag diff."""
        for tag in prepared.to_add:
            self._tags_repo.add(
                transaction_id=prepared.transaction_id,
                tag=tag,
                actor=actor,
                in_outer_txn=in_outer_txn,
            )
        for tag in prepared.to_remove:
            self._tags_repo.remove(
                transaction_id=prepared.transaction_id,
                tag=tag,
                actor=actor,
                in_outer_txn=in_outer_txn,
            )

    def rename_tag(self, old_tag: str, new_tag: str, *, actor: str) -> TagRenameResult:
        """Rename a tag globally; emit one parent + N child audit events.

        The parent ``tag.rename`` event has ``target_id=None`` since it spans
        many rows; each per-row update emits a ``tag.rename_row`` child whose
        ``parent_audit_id`` chains back to the parent (Req 15).
        """
        prepared = self._prepare_tag_rename(old_tag, new_tag)
        self._db.begin()
        try:
            parent_audit_id = self._apply_tag_rename(
                prepared,
                actor=actor,
                in_outer_txn=True,
                record_noop=True,
            )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        logger.info(
            f"tag.rename old={old_tag} new={new_tag} "
            f"row_count={len(prepared.target_ids)} "
            f"actor={actor}"
        )
        return TagRenameResult(
            parent_audit_id=parent_audit_id or "",
            row_count=len(prepared.target_ids),
        )

    def _prepare_tag_rename(
        self,
        old_tag: str,
        new_tag: str,
    ) -> _PreparedTagRename:
        """Validate and resolve one global tag rename."""
        validate_slug(old_tag)
        validate_slug(new_tag)
        rows = self._db.conn.execute(
            """
            SELECT transaction_id
              FROM app.transaction_tags
             WHERE tag = ?
             ORDER BY transaction_id
            """,
            [old_tag],
        ).fetchall()
        target_ids = tuple(str(row[0]) for row in rows)
        if target_ids:
            conflicts = self._db.conn.execute(
                """
                SELECT transaction_id
                  FROM app.transaction_tags
                 WHERE tag = ? AND transaction_id IN (
                    SELECT transaction_id
                      FROM app.transaction_tags
                     WHERE tag = ?
                 )
                """,
                [new_tag, old_tag],
            ).fetchall()
            if conflicts:
                raise UserError(
                    "The tag rename would duplicate an existing tag target.",
                    code="TAG_RENAME_CONFLICT",
                )
        return _PreparedTagRename(
            old_name=old_tag,
            new_name=new_tag,
            target_ids=target_ids,
        )

    def _apply_tag_rename(
        self,
        prepared: _PreparedTagRename,
        *,
        actor: str,
        in_outer_txn: bool,
        record_noop: bool = False,
    ) -> str | None:
        """Apply one prepared global rename and return its parent audit ID."""
        if not prepared.changed and not record_noop:
            return None
        parent = self._audit.record_audit_event(
            action="tag.rename",
            target=(*_AUDIT_TARGET_TAGS, None),
            before={"old_tag": prepared.old_name},
            after={
                "new_tag": prepared.new_name,
                "row_count": len(prepared.target_ids),
            },
            actor=actor,
        )
        for transaction_id in prepared.target_ids:
            self._tags_repo.rename_row(
                transaction_id=transaction_id,
                old_tag=prepared.old_name,
                new_tag=prepared.new_name,
                actor=actor,
                parent_audit_id=parent.audit_id,
                in_outer_txn=in_outer_txn,
            )
        return parent.audit_id

    def list_tags(self, transaction_id: str) -> list[str]:
        """Return the tags applied to a transaction in lexicographic order."""
        rows = self._db.conn.execute(
            """
            SELECT tag FROM app.transaction_tags
             WHERE transaction_id = ?
             ORDER BY tag
            """,
            [transaction_id],
        ).fetchall()
        return [str(r[0]) for r in rows]

    def list_distinct_tags(self) -> list[tuple[str, int]]:
        """Return ``(tag, usage_count)`` pairs sorted by tag.

        ``usage_count`` is the number of rows in ``app.transaction_tags`` —
        i.e. the number of (transaction, tag) applications.
        """
        rows = self._db.conn.execute(
            """
            SELECT tag, COUNT(*) AS usage_count
              FROM app.transaction_tags
             GROUP BY tag
             ORDER BY tag
            """
        ).fetchall()
        return [(str(r[0]), int(r[1])) for r in rows]

    def list_notes(self, transaction_id: str) -> list[Note]:
        """Return all notes for a transaction in chronological order."""
        rows = self._db.conn.execute(
            """
            SELECT note_id, transaction_id, text, author, created_at
              FROM app.transaction_notes
             WHERE transaction_id = ?
             ORDER BY created_at, note_id
            """,
            [transaction_id],
        ).fetchall()
        return [_row_to_note(r) for r in rows]

    # ------------------------------------------------------------------
    # Splits (curator-style allocations of one parent across categories;
    # spec Req 17–21). Sum of children should equal parent.amount but is
    # warn-not-block: callers use ``splits_balance`` to surface the residual.
    # ------------------------------------------------------------------

    def add_split(
        self,
        transaction_id: str,
        amount: Decimal,
        *,
        category: str | None = None,
        subcategory: str | None = None,
        note: str | None = None,
        actor: str,
    ) -> Split:
        """Append a split to a transaction; emit ``split.add`` audit event.

        Generates a 12-hex truncated UUID4 ``split_id`` and computes the
        next ``ord`` as ``MAX(ord)+1`` for the parent (or 0 when first).
        """
        split_id = uuid.uuid4().hex[:12]
        self._db.begin()
        try:
            ord_row = self._db.conn.execute(
                """
                SELECT COALESCE(MAX(ord) + 1, 0)
                  FROM app.transaction_splits
                 WHERE transaction_id = ?
                """,
                [transaction_id],
            ).fetchone()
            next_ord = int(ord_row[0]) if ord_row is not None else 0
            category_id = resolve_category_id(self._db, category, subcategory)
            self._splits_repo.insert(
                split_id=split_id,
                transaction_id=transaction_id,
                amount=amount,
                category=category,
                subcategory=subcategory,
                category_id=category_id,
                note=note,
                ord=next_ord,
                actor=actor,
                in_outer_txn=True,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        row = self._db.conn.execute(
            """
            SELECT split_id, transaction_id, amount, category, subcategory,
                   note, ord, created_at, created_by
              FROM app.transaction_splits
             WHERE split_id = ?
            """,
            [split_id],
        ).fetchone()
        if row is None:  # defensive — insert just succeeded
            raise RuntimeError(f"split_id={split_id} vanished after insert")
        logger.info(
            f"split.add split_id={split_id} transaction_id={transaction_id} "
            f"actor={actor}"
        )
        return _row_to_split(row)

    def remove_split(self, split_id: str, *, actor: str) -> None:
        """Delete a split; emit ``split.remove`` event with ``after=None``.

        Raises ``LookupError`` if ``split_id`` is unknown.
        """
        self._splits_repo.delete(split_id=split_id, actor=actor)
        logger.info(f"split.remove split_id={split_id} actor={actor}")

    def clear_splits(self, transaction_id: str, *, actor: str) -> None:
        """Delete all splits for a transaction; emit one ``split.remove`` per row.

        Per-row capture (DN3) keeps each split individually undoable. No-op (no
        audit event, no SQL) when the parent has no splits.
        """
        events = self._splits_repo.clear(transaction_id=transaction_id, actor=actor)
        logger.info(
            f"split.clear transaction_id={transaction_id} "
            f"count={len(events)} actor={actor}"
        )

    def set_splits(
        self,
        transaction_id: str,
        splits: list[dict[str, Any]],
        *,
        actor: str,
    ) -> list[Split]:
        """Declarative replace: clear existing splits and add the new sequence atomically.

        Validates every input dict (``amount`` required and Decimal) before
        mutating state so a malformed input never leaves the row set in a
        half-applied state. The clear + adds run in one DuckDB transaction.
        """
        targets: list[_GranularSplitTarget] = []
        for idx, s in enumerate(splits):
            if "amount" not in s:
                raise ValueError(f"splits[{idx}] missing required 'amount'")
            amount = s["amount"]
            if not isinstance(amount, Decimal):
                raise ValueError(
                    f"splits[{idx}].amount must be Decimal, got {type(amount).__name__}"
                )
            targets.append(
                _GranularSplitTarget(
                    amount=amount,
                    category=s.get("category"),
                    subcategory=s.get("subcategory"),
                    note=s.get("note"),
                )
            )
        prepared = self._prepare_splits_set(
            transaction_id,
            targets,
            expected_total=None,
            require_categories=False,
            force_replace=True,
        )
        self._db.begin()
        try:
            self._apply_splits_set(prepared, actor=actor, in_outer_txn=True)
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        logger.info(
            f"split.set transaction_id={transaction_id} "
            f"count={len(prepared.desired)} "
            f"actor={actor}"
        )
        return self.list_splits(transaction_id)

    def _prepare_splits_set(
        self,
        transaction_id: str,
        splits: Sequence[_SplitTargetLike],
        *,
        expected_total: Decimal | None,
        require_categories: bool,
        force_replace: bool = False,
    ) -> _PreparedSplitsSet:
        """Validate and resolve one declarative split sequence."""
        desired: list[_PreparedSplit] = []
        total = Decimal("0")
        for split in splits:
            category_id = resolve_category_id(
                self._db,
                split.category,
                split.subcategory,
            )
            if (
                require_categories
                and split.category is not None
                and category_id is None
            ):
                raise UserError(
                    "The split category reference did not match a category.",
                    code="CATEGORY_REFERENCE_NOT_FOUND",
                )
            desired.append(
                _PreparedSplit(
                    amount=split.amount,
                    category=split.category,
                    subcategory=split.subcategory,
                    category_id=category_id,
                    note=split.note,
                )
            )
            total += split.amount
        if splits and expected_total is not None and total != expected_total:
            raise UserError(
                "Split amounts must total the transaction amount.",
                code="SPLIT_TOTAL_INVALID",
            )
        rows = self._db.conn.execute(
            """
            SELECT amount, category, subcategory, category_id, note
              FROM app.transaction_splits
             WHERE transaction_id = ?
             ORDER BY ord, split_id
            """,
            [transaction_id],
        ).fetchall()
        current = tuple(
            _PreparedSplit(
                amount=(
                    row[0] if isinstance(row[0], Decimal) else Decimal(str(row[0]))
                ),
                category=row[1],
                subcategory=row[2],
                category_id=row[3],
                note=row[4],
            )
            for row in rows
        )
        target = tuple(desired)
        return _PreparedSplitsSet(
            transaction_id=transaction_id,
            current=current,
            desired=target,
            changed=force_replace or current != target,
            destructive=bool(current and (force_replace or current != target)),
        )

    def _apply_splits_set(
        self,
        prepared: _PreparedSplitsSet,
        *,
        actor: str,
        in_outer_txn: bool,
    ) -> None:
        """Apply one prepared split replacement."""
        if not prepared.changed:
            return
        self._splits_repo.clear(
            transaction_id=prepared.transaction_id,
            actor=actor,
            in_outer_txn=in_outer_txn,
        )
        for ord_idx, split in enumerate(prepared.desired):
            self._splits_repo.insert(
                split_id=uuid.uuid4().hex[:12],
                transaction_id=prepared.transaction_id,
                amount=split.amount,
                category=split.category,
                subcategory=split.subcategory,
                category_id=split.category_id,
                note=split.note,
                ord=ord_idx,
                actor=actor,
                in_outer_txn=in_outer_txn,
            )

    def list_splits(self, transaction_id: str) -> list[Split]:
        """Return all splits for a transaction ordered by ``ord, split_id``."""
        rows = self._db.conn.execute(
            """
            SELECT split_id, transaction_id, amount, category, subcategory,
                   note, ord, created_at, created_by
              FROM app.transaction_splits
             WHERE transaction_id = ?
             ORDER BY ord, split_id
            """,
            [transaction_id],
        ).fetchall()
        return [_row_to_split(r) for r in rows]

    def splits_balance(self, transaction_id: str) -> Decimal:
        """Return signed residual ``parent.amount - SUM(children.amount)``.

        Returns ``Decimal("0")`` when the children exactly balance the parent;
        a non-zero signed residual otherwise. Raises ``LookupError`` if the
        parent transaction does not exist in ``core.fct_transactions``.
        """
        row = self._db.conn.execute(
            f"""
            SELECT t.amount - COALESCE((
                SELECT SUM(amount)
                  FROM app.transaction_splits s
                 WHERE s.transaction_id = t.transaction_id
            ), 0) AS residual
              FROM {FCT_TRANSACTIONS.full_name} t
             WHERE t.transaction_id = ?
            """,
            [transaction_id],
        ).fetchone()
        if row is None:
            raise LookupError(f"transaction_id={transaction_id} not found")
        # DuckDB returns DECIMAL columns as ``Decimal`` natively; defend against
        # str-shaped returns from older drivers without losing precision.
        residual = row[0]
        return residual if isinstance(residual, Decimal) else Decimal(str(residual))


def _row_to_split(row: tuple[Any, ...]) -> Split:
    return Split(
        split_id=str(row[0]),
        transaction_id=str(row[1]),
        amount=row[2] if isinstance(row[2], Decimal) else Decimal(str(row[2])),
        category=str(row[3]) if row[3] is not None else None,
        subcategory=str(row[4]) if row[4] is not None else None,
        note=str(row[5]) if row[5] is not None else None,
        ord=int(row[6]),
        created_at=str(row[7]),
        created_by=str(row[8]),
    )


def _row_to_note(row: tuple[Any, ...]) -> Note:
    return Note(
        note_id=str(row[0]),
        transaction_id=str(row[1]),
        text=str(row[2]),
        author=str(row[3]),
        created_at=str(row[4]),
    )
