"""Audited writes to ``app.budgets`` (monthly budget targets by category).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``BudgetService``
composes this instead of raw SQL; the existence-check read and ``category_id``
resolution stay in the service.

``set_budget`` is a read-decide-write upsert keyed on a date-range overlap (not a
PK conflict), so the two write branches surface as :meth:`insert` and
:meth:`update` rather than a single ``ON CONFLICT`` statement — both emit the
``budget.set`` action (insert distinguished by ``before=None``), matching the
``category.set`` upsert taxonomy.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import BUDGETS

_BUDGETS_COLUMNS = (
    "budget_id",
    "category",
    "category_id",
    "monthly_amount",
    "start_month",
    "end_month",
    "created_at",
    "updated_at",
)


class BudgetsRepo(BaseRepo):
    """Audited CRUD over ``app.budgets`` (budget targets)."""

    repository = "budgets"

    _AUDIT_TARGET = (BUDGETS.schema, BUDGETS.name)

    def _fetch_row(self, budget_id: str) -> dict[str, Any] | None:
        return self._fetch_one(BUDGETS, _BUDGETS_COLUMNS, "budget_id", budget_id)

    def insert(
        self,
        *,
        category: str,
        category_id: str | None,
        monthly_amount: Decimal,
        start_month: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a new budget target + audit (``budget.set``, ``before=None``)."""
        budget_id = uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {BUDGETS.full_name}
                    (budget_id, category, category_id, monthly_amount, start_month)
                VALUES (?, ?, ?, ?, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [budget_id, category, category_id, monthly_amount, start_month],
            )
            after = self._fetch_row(budget_id)
            return self._emit_audit(
                action="budget.set",
                target=(*self._AUDIT_TARGET, budget_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def update(
        self,
        budget_id: str,
        *,
        monthly_amount: Decimal,
        category_id: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Update an existing budget's amount + ``category_id`` + audit (``budget.set``).

        Re-resolving ``category_id`` on update heals NULL FKs left by V014's
        backfill window the first time the user touches the budget. Raises
        ``ValueError`` if no budget with this id exists (the before-image guard).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(budget_id), "budget_id", budget_id)
            # `CURRENT_TIMESTAMP` is correct in this plain UPDATE. The sibling
            # repos use `NOW()` only because DuckDB parses `CURRENT_TIMESTAMP` as
            # an identifier inside `ON CONFLICT DO UPDATE SET` — that quirk does
            # not apply here, so do NOT cargo-cult `NOW()` onto this statement.
            self._db.execute(
                f"""
                UPDATE {BUDGETS.full_name}
                SET monthly_amount = ?,
                    category_id = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE budget_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [monthly_amount, category_id, budget_id],
            )
            after = self._fetch_row(budget_id)
            return self._emit_audit(
                action="budget.set",
                target=(*self._AUDIT_TARGET, budget_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
