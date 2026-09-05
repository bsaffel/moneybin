# src/moneybin/services/transaction_service.py
"""Transaction search service.

Business logic for transaction search and filtering.
Consumed by both MCP tools and CLI commands.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Protocol, cast

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.hashing import gold_key_unmatched
from moneybin.protocol.pagination import (
    KeysetPosition,
    KeysetScalar,
    SortDirection,
    build_keyset_page,
    canonical_iso_date,
    decode_keyset_cursor,
    reject_inverted_keyset,
    validate_keyset_shape,
)
from moneybin.protocol.write_contracts import (
    AnnotationRequest,
    NoteAdd,
    NoteDelete,
    NoteEdit,
    SplitsSet,
    TagRename,
    TagsSet,
)
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.repositories.transaction_splits_repo import TransactionSplitsRepo
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
from moneybin.services._validators import (
    validate_category_hierarchy,
    validate_category_text,
    validate_currency_code,
    validate_note_text,
    validate_slug,
)
from moneybin.services.account_resolution_types import matchable_account_name
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization._shared import resolve_category_id
from moneybin.services.mutation_context import operation
from moneybin.tables import (
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    MANUAL_TRANSACTIONS,
    TRANSACTION_NOTES,
    TRANSACTION_SPLITS,
    TRANSACTION_TAGS,
)

logger = logging.getLogger(__name__)

# Namespace for cursors minted by TransactionService.get(). Distinct from the
# MCP `transactions` tool's namespace: same table, different public filter set,
# so a cursor from one must not decode against the other.
_TRANSACTION_LIST_CURSOR = "transactions_list"

# Display order of the keyset: `ORDER BY transaction_date DESC, transaction_id`.
TRANSACTION_KEY_DIRECTIONS: tuple[SortDirection, ...] = ("desc", "asc")


def transaction_keyset_bounds(
    position: KeysetPosition,
) -> tuple[tuple[str, str], tuple[str, str]]:
    """Validate a decoded transaction cursor and narrow it to date/id pairs.

    Shared with the MCP ``transactions`` tool: both surfaces page the same
    ``ORDER BY transaction_date DESC, transaction_id`` walk, so a key one
    rejects the other must reject. Key shape and continuation order come from
    ``validate_keyset_position``; what remains is what this key *means*. A
    well-typed but non-ISO date reaches DuckDB and raises a ConversionException,
    which ``classify_user_error`` does not recognize — the caller gets a
    traceback instead of an envelope. An empty transaction_id is worse than
    malformed: ``transaction_id > ''`` is true for every row, so the
    continuation silently re-serves rows the cursor claims to be past.
    """
    validate_keyset_shape(position, key_types=(str, str))
    snapshot = _canonical_transaction_key(position.snapshot)
    after = _canonical_transaction_key(position.after)
    reject_inverted_keyset(
        KeysetPosition(snapshot=snapshot, after=after, total=position.total),
        TRANSACTION_KEY_DIRECTIONS,
    )
    return snapshot, after


def _canonical_transaction_key(
    key: tuple[KeysetScalar, ...],
) -> tuple[str, str]:
    """Return one transaction key with its day in canonical extended ISO form.

    ``date.fromisoformat`` accepts basic ``20250101`` as readily as extended
    ``2025-01-02``, and those two spellings sort against each other backwards
    from the dates they denote. Comparing raw keys would let a forged cursor
    mixing the two pass the ordering guard while still being inverted, so the
    day is normalized before it reaches either the guard or the SQL predicate.
    """
    day, transaction_id = cast("tuple[str, str]", key)
    if not transaction_id:
        raise ValueError("keyset cursor carries an empty transaction id")
    return canonical_iso_date(day), transaction_id


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


def _state_digest(value: object) -> str:
    """Hash live preflight state without exposing annotation contents."""
    encoded = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


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
    # Null only for rows the source never denominated; a mixed-currency result
    # reports display_currency=None, so this is the only per-row answer.
    currency_code: str | None = None
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
        if self.currency_code is not None:
            d["currency_code"] = self.currency_code
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
    """Result of TransactionService.get().

    ``total_count`` is every row matching the filters, not the page length —
    the same meaning ``summary.total_count`` carries on the MCP surface. It is
    carried on the result rather than recomputed by callers so a truncated
    page cannot present its own size as the match count.
    """

    transactions: list[Transaction]
    next_cursor: str | None
    total_count: int


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

    kind: Literal[
        "note_add",
        "note_edit",
        "note_delete",
        "tags_set",
        "splits_set",
        "tag_rename",
    ]
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
        """Return the number of material annotation changes."""
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
        """Apply one atomic annotation batch after full preflight."""
        with operation(operation_id):
            self._db.begin()
            try:
                plan = self.preview_annotations(requests)
                if verify is not None:
                    verify(plan)
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
        """Resolve an exact annotation plan without mutating state."""
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
        if isinstance(request, NoteAdd):
            validate_note_text(request.text)
            self._annotation_transaction_amount(request.transaction_id)
            return _PreparedAnnotation(
                request=request,
                target_ids=(request.transaction_id,),
                changed=True,
                state_digest=_state_digest({"transaction_id": request.transaction_id}),
            )

        if isinstance(request, (NoteEdit, NoteDelete)):
            if isinstance(request, NoteEdit):
                validate_note_text(request.text)
            current = self._annotation_note(request.note_id)
            changed = (
                current.text != request.text if isinstance(request, NoteEdit) else True
            )
            return _PreparedAnnotation(
                request=request,
                target_ids=(request.note_id,),
                changed=changed,
                destructive=isinstance(request, NoteDelete),
                state_digest=_state_digest(current),
            )

        if isinstance(request, TagsSet):
            mutation = self._prepare_tags_set(request.transaction_id, request.tags)
            if request.tags or not mutation.to_remove:
                self._annotation_transaction_amount(request.transaction_id)
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
            elif isinstance(request, NoteAdd):
                key = None
            elif isinstance(request, (NoteEdit, NoteDelete)):
                key = ("note", request.note_id)
            else:
                key = (request.kind, request.transaction_id)
            if key is not None and key in seen:
                raise UserError(
                    "Annotation requests overlap the same target state.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            if key is not None:
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
                code=error_codes.TRANSACTION_REFERENCE_NOT_FOUND,
            )
        amount = row[0]
        return amount if isinstance(amount, Decimal) else Decimal(str(amount))

    def _annotation_note(self, note_id: str) -> Note:
        """Resolve one note for a coarse lifecycle mutation."""
        row = self._db.conn.execute(
            f"""
            SELECT note_id, transaction_id, text, author, created_at
              FROM {TRANSACTION_NOTES.full_name}
             WHERE note_id = ?
            """,  # noqa: S608  # TableRef constant
            [note_id],
        ).fetchone()
        if row is None:
            raise UserError(
                "The note reference did not match a note.",
                code=error_codes.TRANSACTION_NOTE_NOT_FOUND,
            )
        return _row_to_note(row)

    def _apply_prepared_annotation(
        self,
        prepared: _PreparedAnnotation,
        *,
        actor: str,
    ) -> AnnotationOutcome:
        """Apply one preflighted annotation inside the caller's transaction."""
        request = prepared.request
        if isinstance(request, NoteAdd):
            note_id = uuid.uuid4().hex[:12]
            self._notes_repo.add(
                transaction_id=request.transaction_id,
                note_id=note_id,
                text=request.text,
                actor=actor,
                in_outer_txn=True,
            )
            return AnnotationOutcome(
                kind=request.kind,
                target_ids=(note_id,),
                changed=True,
            )

        if isinstance(request, NoteEdit):
            if prepared.changed:
                self._notes_repo.edit(
                    note_id=request.note_id,
                    text=request.text,
                    actor=actor,
                    in_outer_txn=True,
                )
            return AnnotationOutcome(
                kind=request.kind,
                target_ids=prepared.target_ids,
                changed=prepared.changed,
            )

        if isinstance(request, NoteDelete):
            self._notes_repo.delete(
                note_id=request.note_id,
                actor=actor,
                in_outer_txn=True,
            )
            return AnnotationOutcome(
                kind=request.kind,
                target_ids=prepared.target_ids,
                changed=True,
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
        """Resolve every account reference exactly without partial results."""
        from moneybin.services.account_service import (
            AccountNotFoundError,
            AccountService,
            AmbiguousAccountError,
        )
        from moneybin.services.entity_reference import (
            AmbiguousEntity,
            EntityCandidate,
            MissingEntity,
            resolve_entity_reference,
        )

        placeholders = ", ".join("?" * len(accounts))
        exact_rows = self._db.execute(
            f"SELECT account_id FROM {DIM_ACCOUNTS.full_name} WHERE account_id IN ({placeholders})",  # noqa: S608  # TableRef constant
            accounts,
        ).fetchall()
        exact_ids = {str(r[0]) for r in exact_rows}

        rows = (
            AccountService(self._db)
            .list_accounts(
                include_archived=False,
                type_filter=None,
            )
            .rows
        )
        candidates = [
            EntityCandidate(
                entity_id=row.account_id,
                display_name=matchable_account_name(row.display_name),
                aliases=tuple(
                    value
                    for value in (
                        row.institution_name,
                        row.account_type,
                        row.account_subtype,
                    )
                    if value is not None
                ),
            )
            for row in rows
        ]
        names = {
            candidate.entity_id: candidate.display_name for candidate in candidates
        }

        resolved: list[str] = []
        for account in accounts:
            if account in exact_ids:
                resolved.append(account)
                continue
            resolution = resolve_entity_reference(account, candidates)
            if isinstance(resolution, MissingEntity):
                raise AccountNotFoundError(
                    account,
                    [
                        (candidate.entity_id, candidate.display_name)
                        for candidate in candidates
                    ],
                )
            if isinstance(resolution, AmbiguousEntity):
                raise AmbiguousAccountError(
                    account,
                    list(resolution.candidate_ids),
                    [names[account_id] for account_id in resolution.candidate_ids],
                )
            resolved.append(resolution.entity_id)
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
        are resolved as exact account IDs or unambiguous normalized names and
        aliases. Any unresolved entry rejects the whole filter instead of
        returning a partial result.

        Pagination is keyset, not offset: the cursor carries the immutable
        ``(transaction_date, transaction_id)`` key the last served row had,
        plus the total the first page saw. Offset paging skipped a row when
        anything above the boundary was deleted and repeated one when anything
        prepended — both silent, and both wrong on a financial ledger.
        """
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        scope = self._get_cursor_scope(
            accounts=accounts,
            date_from=date_from,
            date_to=date_to,
            categories=categories,
            amount_min=amount_min,
            amount_max=amount_max,
            description=description,
            uncategorized_only=uncategorized_only,
        )
        position: KeysetPosition | None = None
        snapshot: tuple[str, str] | None = None
        after: tuple[str, str] | None = None
        if cursor is not None:
            try:
                position = decode_keyset_cursor(
                    cursor, namespace=_TRANSACTION_LIST_CURSOR, scope=scope
                )
                snapshot, after = transaction_keyset_bounds(position)
            except ValueError as e:
                raise ValueError("invalid cursor") from e

        account_ids: list[str] | None = None
        if accounts:
            account_ids = self._resolve_account_ids(accounts)
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
            # Over-fetch by one: how build_keyset_page learns there is a next
            # page without a second count query.
            limit=limit + 1,
            offset=0,
            snapshot=snapshot,
            after=after,
        )
        total_count = position.total if position is not None else page.total_count
        transactions, next_cursor = build_keyset_page(
            page.transactions,
            limit=limit,
            key_of=lambda t: (t.transaction_date, t.transaction_id),
            namespace=_TRANSACTION_LIST_CURSOR,
            scope=scope,
            # The canonical local, not position.snapshot: minting from the raw
            # decoded value would carry a caller's non-canonical spelling into
            # every later cursor instead of converging on one form.
            snapshot=snapshot,
            total=total_count,
        )

        logger.info(
            f"Transaction query returned {len(transactions)} of {total_count} "
            f"rows (has_more={next_cursor is not None})"
        )
        return TransactionGetResult(
            transactions=transactions,
            next_cursor=next_cursor,
            total_count=total_count,
        )

    @staticmethod
    def _get_cursor_scope(
        *,
        accounts: list[str] | None,
        date_from: str | None,
        date_to: str | None,
        categories: list[str] | None,
        amount_min: Decimal | None,
        amount_max: Decimal | None,
        description: str | None,
        uncategorized_only: bool,
    ) -> dict[str, object]:
        """Canonicalize the public filters a ``get()`` cursor is bound to.

        Binds the caller's own arguments, not the resolved account IDs: the
        cursor must stop being valid when the *request* changes, and a display
        name that later resolves elsewhere is a changed request.
        """
        return {
            "accounts": sorted(accounts) if accounts else None,
            "amount_max": str(amount_max) if amount_max is not None else None,
            "amount_min": str(amount_min) if amount_min is not None else None,
            "categories": sorted(categories) if categories else None,
            "date_from": date_from,
            "date_to": date_to,
            "description": description,
            "uncategorized_only": uncategorized_only,
        }

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
        snapshot: tuple[str, str] | None = None,
        after: tuple[str, str] | None = None,
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
            snapshot=snapshot,
            after=after,
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
        snapshot: tuple[str, str] | None = None,
        after: tuple[str, str] | None = None,
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

        if snapshot is not None:
            conditions.append(
                "(transaction_date < ? OR "
                "(transaction_date = ? AND transaction_id >= ?))"
            )
            params.extend([snapshot[0], snapshot[0], snapshot[1]])
        count_where = "WHERE " + " AND ".join(conditions) if conditions else ""
        count_params = list(params)
        if after is not None:
            conditions.append(
                "(transaction_date < ? OR "
                "(transaction_date = ? AND transaction_id > ?))"
            )
            params.extend([after[0], after[0], after[1]])
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        total_row = self._db.execute(
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} {count_where}",  # noqa: S608  # TableRef + fixed predicates
            count_params,
        ).fetchone()
        total_count = int(total_row[0]) if total_row is not None else 0
        rows = self._db.execute(
            f"""
            SELECT
                transaction_id, account_id, transaction_date, amount,
                description, memo, source_type, category, subcategory,
                currency_code, notes, tags, splits
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
                    currency_code=str(row[9]) if row[9] else None,
                    notes=[dict(n) for n in row[10]] if row[10] else None,
                    tags=list(row[11]) if row[11] else None,
                    splits=[dict(s) for s in row[12]] if row[12] else None,
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

        from moneybin.loaders import import_log

        results: list[ManualEntryRawResult] = []
        self._db.begin()
        try:
            for entry in prepared:
                source_transaction_id = "manual_" + uuid.uuid4().hex[:12]
                transaction_id = gold_key_unmatched(
                    _MANUAL_SOURCE_TYPE,
                    _MANUAL_SOURCE_ORIGIN,
                    entry["account_id"],
                    source_transaction_id,
                )
                # Persist the predicted ``transaction_id`` alongside the source
                # id so the doctor ``orphan_app_state`` audit can join on it to
                # suppress false-positives for notes/tags written against this
                # row in the window between ``transactions_create`` and the
                # next ``refresh_run`` (which materializes the row in
                # ``core.fct_transactions``). Migration V026 added the column;
                # The canonical helper mirrors the SQLMesh unmatched-row hash.
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
                        entry.get("currency_code"),
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
            import_log.finalize_import(
                self._db,
                import_id,
                status="failed",
                rows_total=0,
                rows_imported=0,
            )
            raise

        # Close the batch this path opened. Without it the row stays 'importing'
        # forever with a NULL completed_at and NULL row counts, which
        # `moneybin import history` / `import_status` cannot tell apart from a
        # genuinely crashed write.
        # Finalized here, before categorization: the raw rows are committed and
        # a later categorization failure explicitly leaves them in place.
        import_log.finalize_import(
            self._db,
            import_id,
            status="complete",
            rows_total=len(results),
            rows_imported=len(results),
        )

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

        currency_code = entry.get("currency_code")
        if currency_code is not None:
            if not isinstance(currency_code, str):
                raise ValueError(f"entries[{idx}].currency_code must be a string")
            try:
                validate_currency_code(currency_code)
            except ValueError as e:
                raise ValueError(f"entries[{idx}].{e}") from e

        # The same pair rules a split takes. This pair reaches
        # `set_category_in_active_txn`, which resolves it against the same dim
        # and writes `subcategory` through verbatim without validating, so an
        # unusable pair became a stored row rather than a refusal. Refusing
        # here rather than at the write keeps the batch all-or-nothing: this
        # runs over every entry before the first insert.
        category = entry.get("category")
        subcategory = entry.get("subcategory")
        for name, value in (("category", category), ("subcategory", subcategory)):
            if value is not None and not isinstance(value, str):
                raise UserError(
                    f"entries[{idx}].{name} must be str, got {type(value).__name__}",
                    code=error_codes.TRANSACTION_INVALID_INPUT,
                )
        try:
            # A supplied blank is refused rather than absorbed. It stores
            # nothing wrong on its own — the row lands uncategorized, which is
            # the right end state — but `add_split` and `create_merchant_core`
            # refuse the identical string, so absorbing it here gave one input
            # two answers depending on which command the user reached for.
            # `None` remains how a caller says "uncategorized"; the
            # `cat_entries` filter below still skips it.
            if category is not None:
                validate_category_text(category, "category")
            if subcategory is not None:
                validate_category_text(subcategory, "subcategory")
            validate_category_hierarchy(category, subcategory, "subcategory")
        except ValueError as e:
            raise UserError(
                f"entries[{idx}].{e}",
                code=error_codes.TRANSACTION_INVALID_INPUT,
            ) from e

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
            "currency_code": currency_code,
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
            f"""
            SELECT note_id, transaction_id, text, author, created_at
              FROM {TRANSACTION_NOTES.full_name}
             WHERE note_id = ?
            """,  # noqa: S608  # TRANSACTION_NOTES is a TableRef constant
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
            f"""
            SELECT note_id, transaction_id, text, author, created_at
              FROM {TRANSACTION_NOTES.full_name}
             WHERE note_id = ?
            """,  # noqa: S608  # TRANSACTION_NOTES is a TableRef constant
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
                    f"SELECT 1 FROM {TRANSACTION_TAGS.full_name} "  # noqa: S608  # TableRef constant
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
                    f"SELECT 1 FROM {TRANSACTION_TAGS.full_name} "  # noqa: S608  # TableRef constant
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
            f"""
            SELECT transaction_id
              FROM {TRANSACTION_TAGS.full_name}
             WHERE tag = ?
             ORDER BY transaction_id
            """,  # noqa: S608  # TRANSACTION_TAGS is a TableRef constant
            [old_tag],
        ).fetchall()
        target_ids = tuple(str(row[0]) for row in rows)
        if target_ids:
            conflicts = self._db.conn.execute(
                f"""
                SELECT transaction_id
                  FROM {TRANSACTION_TAGS.full_name}
                 WHERE tag = ? AND transaction_id IN (
                    SELECT transaction_id
                      FROM {TRANSACTION_TAGS.full_name}
                     WHERE tag = ?
                 )
                """,  # noqa: S608  # TRANSACTION_TAGS is a TableRef constant
                [new_tag, old_tag],
            ).fetchall()
            if conflicts:
                raise UserError(
                    "The tag rename would duplicate an existing tag target.",
                    code=error_codes.TRANSACTION_TAG_RENAME_CONFLICT,
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
            f"""
            SELECT tag FROM {TRANSACTION_TAGS.full_name}
             WHERE transaction_id = ?
             ORDER BY tag
            """,  # noqa: S608  # TRANSACTION_TAGS is a TableRef constant
            [transaction_id],
        ).fetchall()
        return [str(r[0]) for r in rows]

    def list_distinct_tags(self) -> list[tuple[str, int]]:
        """Return ``(tag, usage_count)`` pairs sorted by tag.

        ``usage_count`` is the number of rows in ``app.transaction_tags`` —
        i.e. the number of (transaction, tag) applications.
        """
        rows = self._db.conn.execute(
            f"""
            SELECT tag, COUNT(*) AS usage_count
              FROM {TRANSACTION_TAGS.full_name}
             GROUP BY tag
             ORDER BY tag
            """  # noqa: S608  # TRANSACTION_TAGS is a TableRef constant
        ).fetchall()
        return [(str(r[0]), int(r[1])) for r in rows]

    def list_notes(self, transaction_id: str) -> list[Note]:
        """Return all notes for a transaction in chronological order."""
        rows = self._db.conn.execute(
            f"""
            SELECT note_id, transaction_id, text, author, created_at
              FROM {TRANSACTION_NOTES.full_name}
             WHERE transaction_id = ?
             ORDER BY created_at, note_id
            """,  # noqa: S608  # TRANSACTION_NOTES is a TableRef constant
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
        try:
            if category is not None:
                validate_category_text(category, "category")
            if subcategory is not None:
                validate_category_text(subcategory, "subcategory")
            validate_category_hierarchy(category, subcategory, "subcategory")
        except ValueError as exc:
            raise UserError(
                str(exc), code=error_codes.TRANSACTION_INVALID_INPUT
            ) from exc
        self._db.begin()
        try:
            ord_row = self._db.conn.execute(
                f"""
                SELECT COALESCE(MAX(ord) + 1, 0)
                  FROM {TRANSACTION_SPLITS.full_name}
                 WHERE transaction_id = ?
                """,  # noqa: S608  # TRANSACTION_SPLITS is a TableRef constant
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
            f"""
            SELECT split_id, transaction_id, amount, category, subcategory,
                   note, ord, created_at, created_by
              FROM {TRANSACTION_SPLITS.full_name}
             WHERE split_id = ?
            """,  # noqa: S608  # TRANSACTION_SPLITS is a TableRef constant
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
            # A malformed split is bad input to a write, so it carries the same
            # code its MCP twin raises for the identical shape checks
            # (`mcp.tools.curation._prepare_splits`) rather than falling
            # through the classifier to an infra-shaped code.
            if "amount" not in s:
                raise UserError(
                    f"splits[{idx}] missing required 'amount'",
                    code=error_codes.TRANSACTION_INVALID_INPUT,
                )
            amount = s["amount"]
            if not isinstance(amount, Decimal):
                raise UserError(
                    f"splits[{idx}].amount must be Decimal, got {type(amount).__name__}",
                    code=error_codes.TRANSACTION_INVALID_INPUT,
                )
            category = s.get("category")
            subcategory = s.get("subcategory")
            for field, value in (("category", category), ("subcategory", subcategory)):
                if value is not None and not isinstance(value, str):
                    raise UserError(
                        f"splits[{idx}].{field} must be str, "
                        f"got {type(value).__name__}",
                        code=error_codes.TRANSACTION_INVALID_INPUT,
                    )
            targets.append(
                _GranularSplitTarget(
                    amount=amount,
                    category=category,
                    subcategory=subcategory,
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
        for idx, split in enumerate(splits):
            # The MCP arm reaches here already validated by SplitTarget; the
            # granular `set_splits` arm takes untyped dicts and does not.
            try:
                if split.category is not None:
                    validate_category_text(split.category, f"splits[{idx}].category")
                if split.subcategory is not None:
                    validate_category_text(
                        split.subcategory, f"splits[{idx}].subcategory"
                    )
                validate_category_hierarchy(
                    split.category, split.subcategory, f"splits[{idx}].subcategory"
                )
            except ValueError as exc:
                raise UserError(
                    str(exc), code=error_codes.TRANSACTION_INVALID_INPUT
                ) from exc
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
                    code=error_codes.TAXONOMY_CATEGORY_REFERENCE_NOT_FOUND,
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
                code=error_codes.TRANSACTION_SPLIT_TOTAL_INVALID,
            )
        rows = self._db.conn.execute(
            f"""
            SELECT amount, category, subcategory, category_id, note
              FROM {TRANSACTION_SPLITS.full_name}
             WHERE transaction_id = ?
             ORDER BY ord, split_id
            """,  # noqa: S608  # TRANSACTION_SPLITS is a TableRef constant
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
            f"""
            SELECT split_id, transaction_id, amount, category, subcategory,
                   note, ord, created_at, created_by
              FROM {TRANSACTION_SPLITS.full_name}
             WHERE transaction_id = ?
             ORDER BY ord, split_id
            """,  # noqa: S608  # TRANSACTION_SPLITS is a TableRef constant
            [transaction_id],
        ).fetchall()
        return [_row_to_split(r) for r in rows]

    def get_split(self, split_id: str) -> Split | None:
        """Return one split by id, or None if not found."""
        row = self._splits_repo.get(split_id)
        if row is None:
            return None
        amount = row["amount"]
        return Split(
            split_id=row["split_id"],
            transaction_id=row["transaction_id"],
            amount=amount if isinstance(amount, Decimal) else Decimal(str(amount)),
            category=row["category"],
            subcategory=row["subcategory"],
            note=row["note"],
            ord=row["ord"],
            created_at=str(row["created_at"]),
            created_by=str(row["created_by"]),
        )

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
                  FROM {TRANSACTION_SPLITS.full_name} s
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
