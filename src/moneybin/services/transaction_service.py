# src/moneybin/services/transaction_service.py
"""Transaction search and recurring pattern service.

Business logic for transaction search, filtering, and recurring pattern
detection. Consumed by both MCP tools and CLI commands.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services._validators import validate_note_text, validate_slug
from moneybin.services.audit_service import AuditService
from moneybin.tables import (
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    MANUAL_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)

# Audit target prefix for note operations (schema, table); the third tuple
# element is the per-event transaction_id so chains stitch by entity.
_AUDIT_TARGET_NOTES = ("app", "transaction_notes")
_AUDIT_TARGET_TAGS = ("app", "transaction_tags")
_AUDIT_TARGET_SPLITS = ("app", "transaction_splits")
_AUDIT_TARGET_MANUAL = ("raw", "manual_transactions")
_MANUAL_BATCH_MAX = 100
_MANUAL_FORMAT_NAME = "manual_entry"
_MANUAL_SOURCE_TYPE = "manual"


def _predict_manual_gold_key(source_transaction_id: str, account_id: str) -> str:
    """Pre-compute the gold ``transaction_id`` the SQLMesh pipeline will assign.

    Mirrors the unmatched-row branch of ``int_transactions__matched``:
    ``SUBSTRING(SHA256(source_type || '|' || source_transaction_id || '|' || account_id), 1, 16)``.
    Manual rows are exempt from the matcher (spec Req 6 / Task 8) so this
    branch is the only one they hit. If either side of this hash drifts from
    the SQL, the pre-attached user-category row will silently fail to join in
    ``core.fct_transactions``.
    """
    raw = f"{_MANUAL_SOURCE_TYPE}|{source_transaction_id}|{account_id}"
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
        if self.memo:
            d["memo"] = self.memo
        if self.category:
            d["category"] = self.category
        if self.subcategory:
            d["subcategory"] = self.subcategory
        if self.notes is not None:
            d["notes"] = self.notes
        if self.tags is not None:
            d["tags"] = self.tags
        if self.splits is not None:
            d["splits"] = self.splits
        return d


@dataclass(slots=True)
class TransactionSearchResult:
    """Result of transaction search query."""

    transactions: list[Transaction]
    total_count: int

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[t.to_dict() for t in self.transactions],
            sensitivity="medium",
            total_count=self.total_count,
            actions=[
                "Use transactions_recurring_list to find subscription patterns",
                "Use transactions_categorize_apply to categorize uncategorized transactions",
            ],
        )


@dataclass(frozen=True, slots=True)
class RecurringTransaction:
    """A detected recurring transaction pattern."""

    description: str
    avg_amount: Decimal
    occurrence_count: int
    first_seen: str
    last_seen: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        return {
            "description": self.description,
            "avg_amount": self.avg_amount,
            "occurrence_count": self.occurrence_count,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


@dataclass(slots=True)
class RecurringResult:
    """Result of recurring transaction detection."""

    transactions: list[RecurringTransaction]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[t.to_dict() for t in self.transactions],
            sensitivity="medium",
            actions=[
                "Use transactions_search to see individual occurrences",
                "Use budget_set to create a budget for a recurring expense",
            ],
        )


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
    """Transaction search, recurring patterns, notes, and tag operations.

    Search and recurring helpers return typed dataclasses with a
    ``to_envelope()`` method. Note and tag operations wrap each mutation and
    its audit event(s) in a single DuckDB transaction. Tag writes come in
    imperative (``add_tags``/``remove_tags``) and declarative (``set_tags``)
    flavors; ``rename_tag`` is the one bulk operation and emits a parent
    ``tag.rename`` audit event with per-row ``tag.rename_row`` children.
    """

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Initialize with an open Database; lazily build AuditService if absent.

        ``audit`` is keyword-only so existing positional call sites
        (``TransactionService(db)``) continue to work without modification.
        """
        self._db = db
        self._audit = audit if audit is not None else AuditService(db)

    def search(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        min_amount: Decimal | None = None,
        max_amount: Decimal | None = None,
        description: str | None = None,
        account_id: str | None = None,
        category: str | None = None,
        uncategorized_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> TransactionSearchResult:
        """Search transactions with flexible filtering.

        Args:
            start_date: ISO 8601 start date (inclusive).
            end_date: ISO 8601 end date (inclusive).
            min_amount: Minimum amount filter.
            max_amount: Maximum amount filter.
            description: ILIKE pattern matched against description and memo.
            account_id: Filter to a specific account.
            category: Filter by category (from transaction_categories).
            uncategorized_only: Only return uncategorized transactions.
            limit: Maximum rows to return.
            offset: Number of rows to skip.

        Returns:
            TransactionSearchResult with matching transactions and total count.
        """
        conditions: list[str] = []
        params: list[object] = []

        if start_date:
            conditions.append("t.transaction_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("t.transaction_date <= ?")
            params.append(end_date)
        if min_amount is not None:
            conditions.append("t.amount >= ?")
            params.append(min_amount)
        if max_amount is not None:
            conditions.append("t.amount <= ?")
            params.append(max_amount)
        if description:
            conditions.append("(t.description ILIKE ? OR t.memo ILIKE ?)")
            like_pattern = f"%{description}%"
            params.extend([like_pattern, like_pattern])
        if account_id:
            conditions.append("t.account_id = ?")
            params.append(account_id)
        if category:
            conditions.append("c.category = ?")
            params.append(category)
        if uncategorized_only:
            conditions.append("c.transaction_id IS NULL")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count query (same conditions, no limit/offset)
        count_sql = f"""
            SELECT COUNT(*)
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            {where}
        """
        count_result = self._db.execute(count_sql, params)
        total_count = int(count_result.fetchone()[0])  # type: ignore[index]

        # Data query
        sql = f"""
            SELECT
                t.transaction_id,
                t.account_id,
                t.transaction_date,
                t.amount,
                t.description,
                t.memo,
                t.source_type,
                c.category,
                c.subcategory
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            {where}
            ORDER BY t.transaction_date DESC, t.transaction_id
            LIMIT ? OFFSET ?
        """

        result = self._db.execute(sql, [*params, limit, offset])
        rows = result.fetchall()

        transactions = [
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
            )
            for row in rows
        ]

        logger.info(
            f"Search returned {len(transactions)} of {total_count} transactions"
        )
        return TransactionSearchResult(
            transactions=transactions, total_count=total_count
        )

    def recurring(self, min_occurrences: int = 3) -> RecurringResult:
        """Detect recurring expense transaction patterns (amount < 0).

        Groups expense transactions by description and rounded absolute
        amount to identify subscriptions and recurring charges.

        Args:
            min_occurrences: Minimum number of occurrences to consider
                a transaction as recurring.

        Returns:
            RecurringResult with detected recurring patterns.
        """
        sql = f"""
            SELECT
                description,
                AVG(amount) AS avg_amount,
                COUNT(*) AS occurrence_count,
                MIN(transaction_date) AS first_seen,
                MAX(transaction_date) AS last_seen
            FROM {FCT_TRANSACTIONS.full_name}
            WHERE amount < 0
            GROUP BY description, ROUND(ABS(amount), 0)
            HAVING COUNT(*) >= ?
            ORDER BY occurrence_count DESC, description
        """

        result = self._db.execute(sql, [min_occurrences])
        rows = result.fetchall()

        transactions = [
            RecurringTransaction(
                description=str(row[0]),
                avg_amount=Decimal(str(row[1])),
                occurrence_count=int(row[2]),
                first_seen=str(row[3]),
                last_seen=str(row[4]),
            )
            for row in rows
        ]

        logger.info(f"Found {len(transactions)} recurring patterns")
        return RecurringResult(transactions=transactions)

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
                self._db.conn.execute(
                    f"""
                    INSERT INTO {MANUAL_TRANSACTIONS.full_name} (
                        source_transaction_id, import_id, account_id,
                        transaction_date, amount, description, merchant_name,
                        memo, category, subcategory, payment_channel,
                        transaction_type, check_number, currency_code,
                        created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        from moneybin.services.categorization_service import CategorizationService

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
        # Explicit begin/commit — DuckDB's connection context-manager closes the
        # connection on exit, which would invalidate the long-lived process-wide
        # connection. We pair the mutation and audit insert atomically instead.
        self._db.begin()
        try:
            self._db.conn.execute(
                """
                INSERT INTO app.transaction_notes
                    (note_id, transaction_id, text, author)
                VALUES (?, ?, ?, ?)
                """,
                [note_id, transaction_id, text, actor],
            )
            self._audit.record_audit_event(
                action="note.add",
                target=(*_AUDIT_TARGET_NOTES, transaction_id),
                before=None,
                after={"note_id": note_id, "text": text, "author": actor},
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
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
        self._db.begin()
        try:
            existing = self._db.conn.execute(
                """
                SELECT transaction_id, text
                  FROM app.transaction_notes
                 WHERE note_id = ?
                """,
                [note_id],
            ).fetchone()
            if existing is None:
                raise LookupError(f"note_id={note_id} not found")
            txn_id, prior = existing[0], existing[1]
            self._db.conn.execute(
                "UPDATE app.transaction_notes SET text = ? WHERE note_id = ?",
                [text, note_id],
            )
            self._audit.record_audit_event(
                action="note.edit",
                target=(*_AUDIT_TARGET_NOTES, txn_id),
                before={"text": prior},
                after={"text": text},
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
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
        self._db.begin()
        try:
            existing = self._db.conn.execute(
                """
                SELECT transaction_id, text, author
                  FROM app.transaction_notes
                 WHERE note_id = ?
                """,
                [note_id],
            ).fetchone()
            if existing is None:
                raise LookupError(f"note_id={note_id} not found")
            txn_id, text, author = existing[0], existing[1], existing[2]
            self._db.conn.execute(
                "DELETE FROM app.transaction_notes WHERE note_id = ?",
                [note_id],
            )
            self._audit.record_audit_event(
                action="note.delete",
                target=(*_AUDIT_TARGET_NOTES, txn_id),
                before={"note_id": note_id, "text": text, "author": author},
                after=None,
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        logger.info(f"note.delete note_id={note_id} actor={actor}")

    # ------------------------------------------------------------------
    # Tags (slug-flavored labels on a transaction; spec Req 13–16)
    # ------------------------------------------------------------------

    def add_tags(
        self, transaction_id: str, tags: list[str], *, actor: str
    ) -> list[str]:
        """Apply tags to a transaction; emit one ``tag.add`` event per tag.

        Idempotent: re-adding an existing tag emits a ``tag.add`` event with
        ``context_json={"noop": True}`` and no row change. All tag patterns
        are validated up front so a bad tag never half-mutates state.
        Returns the list of tags that were actually inserted (excludes no-ops).
        """
        for t in tags:
            validate_slug(t)
        added: list[str] = []
        self._db.begin()
        try:
            for tag in tags:
                existed = self._db.conn.execute(
                    """
                    SELECT 1 FROM app.transaction_tags
                     WHERE transaction_id = ? AND tag = ?
                    """,
                    [transaction_id, tag],
                ).fetchone()
                if existed:
                    self._audit.record_audit_event(
                        action="tag.add",
                        target=(*_AUDIT_TARGET_TAGS, transaction_id),
                        before={"tag": tag},
                        after={"tag": tag},
                        actor=actor,
                        context={"noop": True},
                    )
                    continue
                self._db.conn.execute(
                    """
                    INSERT INTO app.transaction_tags
                        (transaction_id, tag, applied_by)
                    VALUES (?, ?, ?)
                    """,
                    [transaction_id, tag, actor],
                )
                self._audit.record_audit_event(
                    action="tag.add",
                    target=(*_AUDIT_TARGET_TAGS, transaction_id),
                    before=None,
                    after={"tag": tag},
                    actor=actor,
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
        """Remove tags from a transaction; emit one ``tag.remove`` per tag.

        Idempotent: removing an absent tag emits a ``tag.remove`` event with
        ``context_json={"noop": True}`` and no row change. Returns the list
        of tags that were actually deleted.
        """
        removed: list[str] = []
        self._db.begin()
        try:
            for tag in tags:
                existed = self._db.conn.execute(
                    """
                    SELECT 1 FROM app.transaction_tags
                     WHERE transaction_id = ? AND tag = ?
                    """,
                    [transaction_id, tag],
                ).fetchone()
                if not existed:
                    self._audit.record_audit_event(
                        action="tag.remove",
                        target=(*_AUDIT_TARGET_TAGS, transaction_id),
                        before=None,
                        after=None,
                        actor=actor,
                        context={"noop": True},
                    )
                    continue
                self._db.conn.execute(
                    """
                    DELETE FROM app.transaction_tags
                     WHERE transaction_id = ? AND tag = ?
                    """,
                    [transaction_id, tag],
                )
                self._audit.record_audit_event(
                    action="tag.remove",
                    target=(*_AUDIT_TARGET_TAGS, transaction_id),
                    before={"tag": tag},
                    after=None,
                    actor=actor,
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
        for t in tags:
            validate_slug(t)
        desired = set(tags)
        self._db.begin()
        try:
            current_rows = self._db.conn.execute(
                "SELECT tag FROM app.transaction_tags WHERE transaction_id = ?",
                [transaction_id],
            ).fetchall()
            current = {r[0] for r in current_rows}
            to_add = sorted(desired - current)
            to_remove = sorted(current - desired)
            for tag in to_add:
                self._db.conn.execute(
                    """
                    INSERT INTO app.transaction_tags
                        (transaction_id, tag, applied_by)
                    VALUES (?, ?, ?)
                    """,
                    [transaction_id, tag, actor],
                )
                self._audit.record_audit_event(
                    action="tag.add",
                    target=(*_AUDIT_TARGET_TAGS, transaction_id),
                    before=None,
                    after={"tag": tag},
                    actor=actor,
                )
            for tag in to_remove:
                self._db.conn.execute(
                    """
                    DELETE FROM app.transaction_tags
                     WHERE transaction_id = ? AND tag = ?
                    """,
                    [transaction_id, tag],
                )
                self._audit.record_audit_event(
                    action="tag.remove",
                    target=(*_AUDIT_TARGET_TAGS, transaction_id),
                    before={"tag": tag},
                    after=None,
                    actor=actor,
                )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        logger.info(
            f"tag.set transaction_id={transaction_id} added={len(to_add)} "
            f"removed={len(to_remove)} actor={actor}"
        )
        return sorted(desired)

    def rename_tag(self, old_tag: str, new_tag: str, *, actor: str) -> TagRenameResult:
        """Rename a tag globally; emit one parent + N child audit events.

        The parent ``tag.rename`` event has ``target_id=None`` since it spans
        many rows; each per-row update emits a ``tag.rename_row`` child whose
        ``parent_audit_id`` chains back to the parent (Req 15).
        """
        validate_slug(old_tag)
        validate_slug(new_tag)
        self._db.begin()
        try:
            rows = self._db.conn.execute(
                "SELECT transaction_id FROM app.transaction_tags WHERE tag = ?",
                [old_tag],
            ).fetchall()
            parent = self._audit.record_audit_event(
                action="tag.rename",
                target=(*_AUDIT_TARGET_TAGS, None),
                before={"old_tag": old_tag},
                after={"new_tag": new_tag, "row_count": len(rows)},
                actor=actor,
            )
            for (txn_id,) in rows:
                self._db.conn.execute(
                    """
                    UPDATE app.transaction_tags
                       SET tag = ?
                     WHERE transaction_id = ? AND tag = ?
                    """,
                    [new_tag, txn_id, old_tag],
                )
                self._audit.record_audit_event(
                    action="tag.rename_row",
                    target=(*_AUDIT_TARGET_TAGS, txn_id),
                    before={"tag": old_tag},
                    after={"tag": new_tag},
                    actor=actor,
                    parent_audit_id=parent.audit_id,
                )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        logger.info(
            f"tag.rename old={old_tag} new={new_tag} row_count={len(rows)} "
            f"actor={actor}"
        )
        return TagRenameResult(parent_audit_id=parent.audit_id, row_count=len(rows))

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
            params: list[Any] = [
                split_id,
                transaction_id,
                amount,
                category,
                subcategory,
                note,
                next_ord,
                actor,
            ]
            self._db.conn.execute(
                """
                INSERT INTO app.transaction_splits
                    (split_id, transaction_id, amount, category, subcategory,
                     note, ord, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            self._audit.record_audit_event(
                action="split.add",
                target=(*_AUDIT_TARGET_SPLITS, transaction_id),
                before=None,
                # ``Decimal`` is not JSON-serializable by default; render as a
                # string so the audit row round-trips faithfully.
                after={
                    "split_id": split_id,
                    "amount": str(amount),
                    "category": category,
                },
                actor=actor,
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
        self._db.begin()
        try:
            existing = self._db.conn.execute(
                """
                SELECT transaction_id, amount, category
                  FROM app.transaction_splits
                 WHERE split_id = ?
                """,
                [split_id],
            ).fetchone()
            if existing is None:
                raise LookupError(f"split_id={split_id} not found")
            txn_id, amount, category = existing[0], existing[1], existing[2]
            self._db.conn.execute(
                "DELETE FROM app.transaction_splits WHERE split_id = ?",
                [split_id],
            )
            self._audit.record_audit_event(
                action="split.remove",
                target=(*_AUDIT_TARGET_SPLITS, txn_id),
                before={
                    "split_id": split_id,
                    "amount": str(amount),
                    "category": category,
                },
                after=None,
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        logger.info(f"split.remove split_id={split_id} actor={actor}")

    def clear_splits(self, transaction_id: str, *, actor: str) -> None:
        """Delete all splits for a transaction; emit one ``split.clear`` event.

        No-op (no audit event, no SQL) when the parent has no splits.
        """
        self._db.begin()
        try:
            count_row = self._db.conn.execute(
                """
                SELECT COUNT(*)
                  FROM app.transaction_splits
                 WHERE transaction_id = ?
                """,
                [transaction_id],
            ).fetchone()
            count = int(count_row[0]) if count_row is not None else 0
            if count == 0:
                self._db.commit()
                return
            self._db.conn.execute(
                "DELETE FROM app.transaction_splits WHERE transaction_id = ?",
                [transaction_id],
            )
            self._audit.record_audit_event(
                action="split.clear",
                target=(*_AUDIT_TARGET_SPLITS, transaction_id),
                before={"split_count": count},
                after=None,
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        logger.info(
            f"split.clear transaction_id={transaction_id} count={count} actor={actor}"
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
        prepared: list[dict[str, Any]] = []
        for idx, s in enumerate(splits):
            if "amount" not in s:
                raise ValueError(f"splits[{idx}] missing required 'amount'")
            amount = s["amount"]
            if not isinstance(amount, Decimal):
                raise ValueError(
                    f"splits[{idx}].amount must be Decimal, got {type(amount).__name__}"
                )
            prepared.append({
                "amount": amount,
                "category": s.get("category"),
                "subcategory": s.get("subcategory"),
                "note": s.get("note"),
            })
        self._db.begin()
        try:
            count_row = self._db.conn.execute(
                """
                SELECT COUNT(*)
                  FROM app.transaction_splits
                 WHERE transaction_id = ?
                """,
                [transaction_id],
            ).fetchone()
            count = int(count_row[0]) if count_row is not None else 0
            if count > 0:
                self._db.conn.execute(
                    "DELETE FROM app.transaction_splits WHERE transaction_id = ?",
                    [transaction_id],
                )
                self._audit.record_audit_event(
                    action="split.clear",
                    target=(*_AUDIT_TARGET_SPLITS, transaction_id),
                    before={"split_count": count},
                    after=None,
                    actor=actor,
                )
            for ord_idx, s in enumerate(prepared):
                split_id = uuid.uuid4().hex[:12]
                self._db.conn.execute(
                    """
                    INSERT INTO app.transaction_splits
                        (split_id, transaction_id, amount, category, subcategory,
                         note, ord, created_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        split_id,
                        transaction_id,
                        s["amount"],
                        s["category"],
                        s["subcategory"],
                        s["note"],
                        ord_idx,
                        actor,
                    ],
                )
                self._audit.record_audit_event(
                    action="split.add",
                    target=(*_AUDIT_TARGET_SPLITS, transaction_id),
                    before=None,
                    after={
                        "split_id": split_id,
                        "amount": str(s["amount"]),
                        "category": s["category"],
                    },
                    actor=actor,
                )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        logger.info(
            f"split.set transaction_id={transaction_id} count={len(prepared)} "
            f"actor={actor}"
        )
        return self.list_splits(transaction_id)

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
