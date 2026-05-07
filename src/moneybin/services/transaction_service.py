# src/moneybin/services/transaction_service.py
"""Transaction search and recurring pattern service.

Business logic for transaction search, filtering, and recurring pattern
detection. Consumed by both MCP tools and CLI commands.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services._validators import validate_note_text, validate_slug
from moneybin.services.audit_service import AuditService
from moneybin.tables import FCT_TRANSACTIONS, TRANSACTION_CATEGORIES

logger = logging.getLogger(__name__)

# Audit target prefix for note operations (schema, table); the third tuple
# element is the per-event transaction_id so chains stitch by entity.
_AUDIT_TARGET_NOTES = ("app", "transaction_notes")
_AUDIT_TARGET_TAGS = ("app", "transaction_tags")


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
class TagRenameResult:
    """Result of ``rename_tag``: the parent audit_id and how many rows shifted."""

    parent_audit_id: str
    row_count: int


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


def _row_to_note(row: tuple[Any, ...]) -> Note:
    return Note(
        note_id=str(row[0]),
        transaction_id=str(row[1]),
        text=str(row[2]),
        author=str(row[3]),
        created_at=str(row[4]),
    )
