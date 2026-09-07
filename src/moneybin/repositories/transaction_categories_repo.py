"""Audited writes to ``app.transaction_categories`` (per-transaction categories).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``CategorizationService``
(via its applier) and ``AutoRuleService`` compose this instead of raw SQL.

Two upsert shapes share the ``category.set`` action — the established curation
audit verb (transaction-curation.md) is preserved, not renamed:

- :meth:`set` — the user-manual-edit path: a partial-column upsert that leaves
  ``merchant_id`` / ``rule_id`` / ``confidence`` untouched on conflict.
- :meth:`update_links` — the maintenance path: updates only ``merchant_id`` and
  ``rule_id`` while preserving the category metadata.
- :meth:`upsert_guarded` — the engine path: a full-column upsert gated by the
  source-precedence ladder, so a lower-authority source never overwrites a
  higher one. The precedence CASE is generated from ``SOURCE_PRIORITY`` (the
  table's write contract), so importing it here keeps the SQL and Python
  ladders in lockstep.
"""

from __future__ import annotations

from typing import Any

from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import TRANSACTION_CATEGORIES

_TRANSACTION_CATEGORIES_COLUMNS = (
    "transaction_id",
    "category",
    "subcategory",
    "category_id",
    "categorized_at",
    "categorized_by",
    "merchant_id",
    "confidence",
    "rule_id",
    "source_type",
)


class TransactionCategoriesRepo(BaseRepo):
    """Audited CRUD over ``app.transaction_categories``."""

    repository = "transaction_categories"

    table_ref = TRANSACTION_CATEGORIES
    pk_columns = ("transaction_id",)

    def _fetch_row(self, transaction_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            TRANSACTION_CATEGORIES,
            _TRANSACTION_CATEGORIES_COLUMNS,
            "transaction_id",
            transaction_id,
        )

    def set(
        self,
        transaction_id: str,
        *,
        category: str,
        subcategory: str | None,
        category_id: str | None,
        categorized_by: str = "user",
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Unconditional user upsert; leaves merchant_id/rule_id/confidence intact.

        The user-manual-edit path. Captures the full prior row (or ``None``) as
        ``before`` and the full resulting row as ``after``; ``merchant_id`` /
        ``rule_id`` / ``confidence`` are not in the SET list, so a conflict
        retains their prior values. ``source_type`` IS reset to ``'internal'``:
        a manual edit is an internal categorization, so overwriting a prior
        provider-native row (e.g. ``source_type='plaid'``) must clear the
        provider-origin tag or stats grouping by ``source_type`` would keep
        counting the now-user-authored row as provider-native.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(transaction_id)
            self._db.execute(
                f"""
                INSERT INTO {TRANSACTION_CATEGORIES.full_name}
                    (transaction_id, category, subcategory, category_id,
                     categorized_at, categorized_by, source_type)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 'internal')
                ON CONFLICT (transaction_id) DO UPDATE SET
                    category = EXCLUDED.category,
                    subcategory = EXCLUDED.subcategory,
                    category_id = EXCLUDED.category_id,
                    categorized_at = EXCLUDED.categorized_at,
                    categorized_by = EXCLUDED.categorized_by,
                    source_type = EXCLUDED.source_type
                """,  # noqa: S608  # TableRef + parameterized values
                [transaction_id, category, subcategory, category_id, categorized_by],
            )
            after = self._fetch_row(transaction_id)
            return self._emit_audit(
                action="category.set",
                target=(*self._audit_target, transaction_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def update_links(
        self,
        transaction_id: str,
        *,
        merchant_id: str | None,
        rule_id: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Update categorization links without changing category metadata."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(transaction_id),
                "transaction_id",
                transaction_id,
            )
            self._db.execute(
                f"UPDATE {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + parameterized values
                "SET merchant_id = ?, rule_id = ? WHERE transaction_id = ?",
                [merchant_id, rule_id, transaction_id],
            )
            after = self._require(
                self._fetch_row(transaction_id),
                "transaction_id",
                transaction_id,
            )
            return self._emit_audit(
                action="category.set",
                target=(*self._audit_target, transaction_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def upsert_guarded(
        self,
        transaction_id: str,
        *,
        category: str,
        subcategory: str | None,
        category_id: str | None,
        categorized_by: str,
        merchant_id: str | None,
        rule_id: str | None,
        confidence: float | None,
        source_type: str = "internal",
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Precedence-guarded engine upsert; ``None`` when the write is skipped.

        The write lands only when the incoming source's priority is at least as
        authoritative as the existing row's (lower number = higher authority),
        enforced atomically in the ``ON CONFLICT … WHERE`` guard. A
        precedence-skipped call mutates nothing and emits no audit.
        """
        # Deferred import: ``_shared`` lives under the ``services.categorization``
        # package, whose __init__ imports the applier (which imports this repo).
        # A module-level import would form a cycle; by call time the package is
        # initialized. The precedence ladder is the table's write contract, so
        # the CASE is generated from the same SOURCE_PRIORITY the engine uses.
        from moneybin.services.categorization._shared import (  # noqa: PLC0415
            priority_case_sql,
        )

        excluded_priority = priority_case_sql("EXCLUDED.categorized_by")
        existing_priority = priority_case_sql(
            f"{TRANSACTION_CATEGORIES.full_name}.categorized_by"
        )
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(transaction_id)
            wrote = self._db.execute(
                f"""
                INSERT INTO {TRANSACTION_CATEGORIES.full_name}
                    (transaction_id, category, subcategory, category_id,
                     categorized_at, categorized_by, merchant_id, rule_id,
                     confidence, source_type)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    category = EXCLUDED.category,
                    subcategory = EXCLUDED.subcategory,
                    category_id = EXCLUDED.category_id,
                    categorized_at = EXCLUDED.categorized_at,
                    categorized_by = EXCLUDED.categorized_by,
                    merchant_id = EXCLUDED.merchant_id,
                    rule_id = EXCLUDED.rule_id,
                    confidence = EXCLUDED.confidence,
                    source_type = EXCLUDED.source_type
                WHERE {excluded_priority} <= {existing_priority}
                RETURNING transaction_id
                """,  # noqa: S608  # TableRef + CASE from SOURCE_PRIORITY + parameterized values
                [
                    transaction_id,
                    category,
                    subcategory,
                    category_id,
                    categorized_by,
                    merchant_id,
                    rule_id,
                    confidence,
                    source_type,
                ],
            ).fetchone()
            # DuckDB returns no rows from RETURNING when the ON CONFLICT … WHERE
            # guard blocks the update, so `wrote is None` means precedence
            # skipped the write. (PostgreSQL 15 changed this to return the
            # existing row; DuckDB tracks PG semantics, so pin the assumption
            # here in case it ever diverges.)
            if wrote is None:
                return None  # precedence-skipped: no mutation, no audit
            after = self._fetch_row(transaction_id)
            return self._emit_audit(
                action="category.set",
                target=(*self._audit_target, transaction_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def upsert_guarded_many(
        self,
        categorizations: list[dict[str, Any]],
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> set[str]:
        """Apply guarded engine categorizations with one write and row-grain audits."""
        if not categorizations:
            return set()
        from moneybin.services.categorization._shared import (  # noqa: PLC0415
            priority_case_sql,
        )

        transaction_ids = [str(item["transaction_id"]) for item in categorizations]
        if len(set(transaction_ids)) != len(transaction_ids):
            raise ValueError("batch categorization requires unique transaction_ids")
        placeholders = ", ".join("?" for _ in transaction_ids)
        columns = ", ".join(quote_ident(c) for c in _TRANSACTION_CATEGORIES_COLUMNS)
        values = ", ".join(
            "(?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)" for _ in categorizations
        )
        params: list[Any] = []
        for item in categorizations:
            params.extend([
                item["transaction_id"],
                item["category"],
                item["subcategory"],
                item["category_id"],
                item["categorized_by"],
                item["merchant_id"],
                item["rule_id"],
                item["confidence"],
                item["source_type"],
            ])
        excluded_priority = priority_case_sql("EXCLUDED.categorized_by")
        existing_priority = priority_case_sql(
            f"{TRANSACTION_CATEGORIES.full_name}.categorized_by"
        )
        with self._transaction(in_outer_txn=in_outer_txn):
            before_rows = self._db.execute(
                f"SELECT {columns} FROM {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + code-constant columns
                f"WHERE transaction_id IN ({placeholders})",
                transaction_ids,
            ).fetchall()
            before_by_id: dict[str, dict[str, Any]] = {
                str(row[0]): dict(
                    zip(_TRANSACTION_CATEGORIES_COLUMNS, row, strict=True)
                )
                for row in before_rows
            }
            wrote = self._db.execute(
                f"""
                INSERT INTO {TRANSACTION_CATEGORIES.full_name}
                    (transaction_id, category, subcategory, category_id,
                     categorized_at, categorized_by, merchant_id, rule_id,
                     confidence, source_type)
                VALUES {values}
                ON CONFLICT (transaction_id) DO UPDATE SET
                    category = EXCLUDED.category,
                    subcategory = EXCLUDED.subcategory,
                    category_id = EXCLUDED.category_id,
                    categorized_at = EXCLUDED.categorized_at,
                    categorized_by = EXCLUDED.categorized_by,
                    merchant_id = EXCLUDED.merchant_id,
                    rule_id = EXCLUDED.rule_id,
                    confidence = EXCLUDED.confidence,
                    source_type = EXCLUDED.source_type
                WHERE {excluded_priority} <= {existing_priority}
                RETURNING transaction_id
                """,  # noqa: S608  # TableRef, CASE, and placeholder count are code controlled
                params,
            ).fetchall()
            written_ids = {str(row[0]) for row in wrote}
            if not written_ids:
                return set()
            written_placeholders = ", ".join("?" for _ in written_ids)
            after_rows = self._db.execute(
                f"SELECT {columns} FROM {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + code-constant columns
                f"WHERE transaction_id IN ({written_placeholders})",
                sorted(written_ids),
            ).fetchall()
            after_by_id: dict[str, dict[str, Any]] = {
                str(row[0]): dict(
                    zip(_TRANSACTION_CATEGORIES_COLUMNS, row, strict=True)
                )
                for row in after_rows
            }
            self._emit_audits(
                action="category.set",
                changes=[
                    (
                        (*self._audit_target, transaction_id),
                        self._serialize_for_audit(before_by_id.get(transaction_id)),
                        self._serialize_for_audit(after_by_id[transaction_id]),
                    )
                    for transaction_id in sorted(written_ids)
                ],
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
            return written_ids

    def clear(
        self,
        transaction_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Delete one transaction's category; ``None`` when there's nothing to clear."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(transaction_id)
            if before is None:
                return None
            self._db.execute(
                f"DELETE FROM {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + parameterized value
                f"WHERE transaction_id = ?",
                [transaction_id],
            )
            return self._emit_audit(
                action="category.clear",
                target=(*self._audit_target, transaction_id),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete_by_rule(
        self,
        rule_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> list[AuditEvent]:
        """Strip rule/auto_rule categorizations a now-inactive rule wrote.

        One ``category.clear`` audit per deleted row, each capturing that row's
        full prior state (Req 4). Higher-priority sources (user/migration/ml/
        provider_native) referencing this ``rule_id`` are left intact.
        """
        cols = ", ".join(quote_ident(c) for c in _TRANSACTION_CATEGORIES_COLUMNS)
        with self._transaction(in_outer_txn=in_outer_txn):
            rows = self._db.execute(
                f"SELECT {cols} FROM {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + code-constant columns + parameterized value
                f"WHERE rule_id = ? AND categorized_by IN ('rule', 'auto_rule')",
                [rule_id],
            ).fetchall()
            if not rows:
                return []
            self._db.execute(
                f"DELETE FROM {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + parameterized value
                f"WHERE rule_id = ? AND categorized_by IN ('rule', 'auto_rule')",
                [rule_id],
            )
            events: list[AuditEvent] = []
            for row in rows:
                before: dict[str, Any] = dict(
                    zip(_TRANSACTION_CATEGORIES_COLUMNS, row, strict=True)
                )
                events.append(
                    self._emit_audit(
                        action="category.clear",
                        target=(*self._audit_target, str(before["transaction_id"])),
                        before=self._serialize_for_audit(before),
                        after=None,
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                    )
                )
            return events

    def delete_by_category(
        self,
        category_id: str,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> list[AuditEvent]:
        """Delete every categorization using one category, with per-row audit."""
        with self._transaction(in_outer_txn=in_outer_txn):
            transaction_ids = [
                str(row[0])
                for row in self._db.execute(
                    f"SELECT transaction_id FROM {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + parameterized value
                    "WHERE category_id = ? ORDER BY transaction_id",
                    [category_id],
                ).fetchall()
            ]
            events: list[AuditEvent] = []
            for transaction_id in transaction_ids:
                event = self.clear(
                    transaction_id,
                    actor=actor,
                    in_outer_txn=True,
                )
                if event is not None:
                    events.append(event)
            return events

    def repoint_transaction(
        self,
        *,
        old_transaction_id: str,
        new_transaction_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> tuple[AuditEvent, ...]:
        """Move a categorization off a superseded ``transaction_id`` onto its successor.

        Called when a dedup merge or a Plaid pending→posted transition re-keys the
        canonical transaction (ADR-015); left behind, the row references an id
        absent from ``core.fct_transactions`` and the ``app_transaction_categories_fk``
        invariant fails.

        ``transaction_id`` is the whole primary key, so when both sides were
        categorized only one row can survive. The winner is decided by the same
        ``SOURCE_PRIORITY`` ladder ``upsert_guarded`` enforces — a merge must not
        let a lower-authority categorization displace a higher one, and must not
        drop the user's edit in favour of the anchor's provider default.

        Because ``transaction_id`` is the whole primary key, a move is audited
        as a delete on the old id plus an insert on the new one rather than as
        one update — the audit target is the row identity, and the cascade check
        that guards undo can only see a change on a key it is named on.

        **A tie moves the superseded row**, matching ``upsert_guarded``, whose
        guard admits an incoming write of equal authority. Two ``user`` edits on
        the two halves of a merge are both the user's, and neither the id nor
        the ladder ranks one above the other; taking the incoming one keeps the
        two write paths answering a tie the same way, which is the property that
        matters when the alternative is a second rule to remember.
        """
        # Deferred import: the ``services.categorization`` package's __init__
        # imports the applier, which imports this repo — a module-level import
        # would cycle. The ladder is the table's write contract, so reusing it
        # keeps the merge tiebreak and the upsert guard from drifting apart.
        from moneybin.services.categorization._shared import (  # noqa: PLC0415
            SOURCE_PRIORITY,
        )

        def rank(row: dict[str, Any]) -> int:
            # An unknown method sorts last: it cannot outrank a declared one.
            return SOURCE_PRIORITY.get(
                str(row["categorized_by"]), len(SOURCE_PRIORITY) + 1
            )

        with self._transaction(in_outer_txn=in_outer_txn):
            before_old = self._fetch_row(old_transaction_id)
            if before_old is None:
                return ()
            events: list[AuditEvent] = []
            before_new = self._fetch_row(new_transaction_id)
            if before_new is not None:
                if rank(before_new) < rank(before_old):
                    # The surviving id already holds the more authoritative row;
                    # the superseded one is dropped rather than moved.
                    self._db.execute(
                        f"DELETE FROM {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + parameterized value
                        f"WHERE transaction_id = ?",
                        [old_transaction_id],
                    )
                    return (
                        self._emit_audit(
                            action="category.repoint_transaction",
                            target=(*self._audit_target, old_transaction_id),
                            before=self._serialize_for_audit(before_old),
                            after=None,
                            actor=actor,
                            parent_audit_id=parent_audit_id,
                        ),
                    )
                self._db.execute(
                    f"DELETE FROM {TRANSACTION_CATEGORIES.full_name} "  # noqa: S608  # TableRef + parameterized value
                    f"WHERE transaction_id = ?",
                    [new_transaction_id],
                )
                events.append(
                    self._emit_audit(
                        action="category.repoint_transaction",
                        target=(*self._audit_target, new_transaction_id),
                        before=self._serialize_for_audit(before_new),
                        after=None,
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                    )
                )
            self._db.execute(
                f"UPDATE {TRANSACTION_CATEGORIES.full_name} SET transaction_id = ? "  # noqa: S608  # TableRef + parameterized values
                f"WHERE transaction_id = ?",
                [new_transaction_id, old_transaction_id],
            )
            # Two row-grain events, not one. ``transaction_id`` IS the primary
            # key, so the move vacates one row identity and creates another, and
            # ``UndoService._cascade_blockers`` joins the audit log on exact
            # ``target_id``: an event naming only the survivor is invisible from
            # the superseded id's side, so undoing the edit that wrote the row
            # would delete by the old id, match nothing, and report success while
            # the moved row stands. Undo replays an operation in reverse write
            # order, so the arrival is reversed before the departure is restored.
            events.append(
                self._emit_audit(
                    action="category.repoint_transaction",
                    target=(*self._audit_target, old_transaction_id),
                    before=self._serialize_for_audit(before_old),
                    after=None,
                    actor=actor,
                    parent_audit_id=parent_audit_id,
                )
            )
            events.append(
                self._emit_audit(
                    action="category.repoint_transaction",
                    target=(*self._audit_target, new_transaction_id),
                    before=None,
                    after=self._serialize_for_audit(
                        self._fetch_row(new_transaction_id)
                    ),
                    actor=actor,
                    parent_audit_id=parent_audit_id,
                )
            )
            return tuple(events)
