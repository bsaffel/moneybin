"""Audited writes to ``app.categorization_rules`` (auto-categorization rules).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. Both the manual rule
writer (``CategorizationService`` via its applier) and the auto-rule promotion
path (``AutoRuleService.approve``) compose this instead of issuing raw SQL.

Category-id resolution stays in the caller (a read against
``core.dim_categories``); the repo receives the resolved ``category_id``.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import CATEGORIZATION_RULES

_CATEGORIZATION_RULES_COLUMNS = (
    "rule_id",
    "name",
    "merchant_pattern",
    "match_type",
    "min_amount",
    "max_amount",
    "account_id",
    "category",
    "subcategory",
    "category_id",
    "priority",
    "is_active",
    "created_by",
    "created_at",
    "updated_at",
)


class CategorizationRulesRepo(BaseRepo):
    """Audited CRUD over ``app.categorization_rules``."""

    repository = "categorization_rules"

    table_ref = CATEGORIZATION_RULES
    pk_columns = ("rule_id",)

    def _fetch_row(self, rule_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            CATEGORIZATION_RULES, _CATEGORIZATION_RULES_COLUMNS, "rule_id", rule_id
        )

    def insert(
        self,
        *,
        name: str,
        merchant_pattern: str,
        match_type: str,
        min_amount: Decimal | float | None,
        max_amount: Decimal | float | None,
        account_id: str | None,
        category: str,
        subcategory: str | None,
        category_id: str | None,
        priority: int,
        created_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert an active rule + audit. ``target_id`` is the new rule id."""
        rule_id = uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {CATEGORIZATION_RULES.full_name}
                    (rule_id, name, merchant_pattern, match_type,
                     min_amount, max_amount, account_id,
                     category, subcategory, category_id, priority, is_active,
                     created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true,
                        ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    rule_id,
                    name,
                    merchant_pattern,
                    match_type,
                    min_amount,
                    max_amount,
                    account_id,
                    category,
                    subcategory,
                    category_id,
                    priority,
                    created_by,
                ],
            )
            after = self._fetch_row(rule_id)
            return self._emit_audit(
                action="categorization_rule.insert",
                target=(*self._audit_target, rule_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def deactivate(
        self,
        rule_id: str,
        *,
        actor: str,
        context: dict[str, Any] | None = None,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Soft-delete a rule (``is_active=false``); capture full before/after.

        The single deactivation path for both manual deletes and the auto-rule
        override path. Callers distinguish *why* via ``context`` (e.g.
        ``{"reason": "override_threshold", "override_count": N}``) rather than a
        separate action name, so the audit taxonomy stays
        ``<entity>.<verb>`` everywhere.

        Returns ``None`` (without emitting audit) when no rule with this id
        exists — the writer contract is lookup-or-noop, not assert-exists, so a
        missing rule is a benign ``False`` rather than a ``ValueError``.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(rule_id)
            if before is None:
                return None
            self._db.execute(
                f"UPDATE {CATEGORIZATION_RULES.full_name} "  # noqa: S608  # TableRef constant
                f"SET is_active = false, updated_at = CURRENT_TIMESTAMP "
                f"WHERE rule_id = ?",
                [rule_id],
            )
            after = self._fetch_row(rule_id)
            return self._emit_audit(
                action="categorization_rule.deactivate",
                target=(*self._audit_target, rule_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
                context=context,
            )

    def set_target(
        self,
        rule_id: str,
        *,
        name: str,
        merchant_pattern: str,
        match_type: str,
        min_amount: Decimal | float | None,
        max_amount: Decimal | float | None,
        account_id: str | None,
        category: str,
        subcategory: str | None,
        category_id: str | None,
        priority: int,
        actor: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Replace one rule's complete active target and audit the before-image."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(rule_id), "rule_id", rule_id)
            self._db.execute(
                f"""
                UPDATE {CATEGORIZATION_RULES.full_name}
                SET name = ?, merchant_pattern = ?, match_type = ?,
                    min_amount = ?, max_amount = ?, account_id = ?,
                    category = ?, subcategory = ?, category_id = ?, priority = ?,
                    is_active = true, updated_at = CURRENT_TIMESTAMP
                WHERE rule_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    name,
                    merchant_pattern,
                    match_type,
                    min_amount,
                    max_amount,
                    account_id,
                    category,
                    subcategory,
                    category_id,
                    priority,
                    rule_id,
                ],
            )
            after = self._fetch_row(rule_id)
            return self._emit_audit(
                action="categorization_rule.set",
                target=(*self._audit_target, rule_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
            )

    def delete(
        self,
        rule_id: str,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Hard-delete one rule while retaining its full audited recovery image."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(rule_id)
            if before is None:
                return None
            self._db.execute(
                f"DELETE FROM {CATEGORIZATION_RULES.full_name} WHERE rule_id = ?",  # noqa: S608  # TableRef + parameterized value
                [rule_id],
            )
            return self._emit_audit(
                action="categorization_rule.delete",
                target=(*self._audit_target, rule_id),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
            )

    def delete_by_category(
        self,
        category_id: str,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> list[AuditEvent]:
        """Delete every rule using one category, with per-row audit."""
        with self._transaction(in_outer_txn=in_outer_txn):
            rule_ids = [
                str(row[0])
                for row in self._db.execute(
                    f"SELECT rule_id FROM {CATEGORIZATION_RULES.full_name} "  # noqa: S608  # TableRef + parameterized value
                    "WHERE category_id = ? ORDER BY rule_id",
                    [category_id],
                ).fetchall()
            ]
            events: list[AuditEvent] = []
            for rule_id in rule_ids:
                event = self.delete(
                    rule_id,
                    actor=actor,
                    in_outer_txn=True,
                )
                if event is not None:
                    events.append(event)
            return events
