"""Audited writes to ``app.transaction_categories`` (per-transaction categories).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``CategorizationService``
(via its applier) and ``AutoRuleService`` compose this instead of raw SQL.

Two upsert shapes share the ``category.set`` action — the established curation
audit verb (transaction-curation.md) is preserved, not renamed:

- :meth:`set` — the user-manual-edit path: a partial-column upsert that leaves
  ``merchant_id`` / ``rule_id`` / ``confidence`` untouched on conflict.
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
)


class TransactionCategoriesRepo(BaseRepo):
    """Audited CRUD over ``app.transaction_categories``."""

    repository = "transaction_categories"

    _AUDIT_TARGET = (TRANSACTION_CATEGORIES.schema, TRANSACTION_CATEGORIES.name)

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
        retains their prior values.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(transaction_id)
            self._db.execute(
                f"""
                INSERT INTO {TRANSACTION_CATEGORIES.full_name}
                    (transaction_id, category, subcategory, category_id,
                     categorized_at, categorized_by)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    category = EXCLUDED.category,
                    subcategory = EXCLUDED.subcategory,
                    category_id = EXCLUDED.category_id,
                    categorized_at = EXCLUDED.categorized_at,
                    categorized_by = EXCLUDED.categorized_by
                """,  # noqa: S608  # TableRef + parameterized values
                [transaction_id, category, subcategory, category_id, categorized_by],
            )
            after = self._fetch_row(transaction_id)
            return self._emit_audit(
                action="category.set",
                target=(*self._AUDIT_TARGET, transaction_id),
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
                     confidence)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    category = EXCLUDED.category,
                    subcategory = EXCLUDED.subcategory,
                    category_id = EXCLUDED.category_id,
                    categorized_at = EXCLUDED.categorized_at,
                    categorized_by = EXCLUDED.categorized_by,
                    merchant_id = EXCLUDED.merchant_id,
                    rule_id = EXCLUDED.rule_id,
                    confidence = EXCLUDED.confidence
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
                target=(*self._AUDIT_TARGET, transaction_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

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
                target=(*self._AUDIT_TARGET, transaction_id),
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
        plaid) referencing this ``rule_id`` are left intact.
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
                        target=(*self._AUDIT_TARGET, str(before["transaction_id"])),
                        before=self._serialize_for_audit(before),
                        after=None,
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                    )
                )
            return events
