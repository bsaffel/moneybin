"""Audited writes to ``app.proposed_rules`` (auto-rule proposal lifecycle).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``AutoRuleService``
composes this instead of issuing raw mutation SQL across the
observe → propose → approve/reject lifecycle.

The proposal lifecycle has five distinct mutation shapes, one method each:
``insert`` (new proposal), ``reinforce`` (trigger-count/status bump),
``supersede`` (category changed under a tracking proposal), ``mark_approved``
(promotion — paired with the rule INSERT via ``parent_audit_id``), and
``mark_rejected``.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import PROPOSED_RULES

_PROPOSED_RULES_COLUMNS = (
    "proposed_rule_id",
    "merchant_pattern",
    "match_type",
    "category",
    "subcategory",
    "category_id",
    "rule_id",
    "status",
    "trigger_count",
    "source",
    "sample_txn_ids",
    "proposed_at",
    "decided_at",
    "decided_by",
)


class ProposedRulesRepo(BaseRepo):
    """Audited CRUD over ``app.proposed_rules``."""

    repository = "proposed_rules"

    _AUDIT_TARGET = (PROPOSED_RULES.schema, PROPOSED_RULES.name)

    def _fetch_row(self, proposed_rule_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            PROPOSED_RULES,
            _PROPOSED_RULES_COLUMNS,
            "proposed_rule_id",
            proposed_rule_id,
        )

    def insert(
        self,
        *,
        merchant_pattern: str,
        match_type: str,
        category: str,
        subcategory: str | None,
        category_id: str | None,
        status: str,
        sample_txn_ids: Sequence[str],
        trigger_count: int = 1,
        source: str = "pattern_detection",
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a new proposal + audit. ``target_id`` is the new proposal id."""
        proposed_rule_id = uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {PROPOSED_RULES.full_name}
                    (proposed_rule_id, merchant_pattern, match_type,
                     category, subcategory, category_id, status,
                     trigger_count, source, sample_txn_ids)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    proposed_rule_id,
                    merchant_pattern,
                    match_type,
                    category,
                    subcategory,
                    category_id,
                    status,
                    trigger_count,
                    source,
                    list(sample_txn_ids),
                ],
            )
            after = self._fetch_row(proposed_rule_id)
            return self._emit_audit(
                action="proposed_rule.insert",
                target=(*self._AUDIT_TARGET, proposed_rule_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def reinforce(
        self,
        proposed_rule_id: str,
        *,
        trigger_count: int,
        sample_txn_ids: Sequence[str],
        status: str,
        category_id: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Bump trigger-count/samples/status on a tracking proposal."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(proposed_rule_id),
                "proposed_rule_id",
                proposed_rule_id,
            )
            self._db.execute(
                f"""
                UPDATE {PROPOSED_RULES.full_name}
                SET trigger_count = ?, sample_txn_ids = ?, status = ?,
                    category_id = ?
                WHERE proposed_rule_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    trigger_count,
                    list(sample_txn_ids),
                    status,
                    category_id,
                    proposed_rule_id,
                ],
            )
            after = self._fetch_row(proposed_rule_id)
            return self._emit_audit(
                action="proposed_rule.reinforce",
                target=(*self._AUDIT_TARGET, proposed_rule_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def supersede(
        self,
        proposed_rule_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Mark a proposal ``superseded`` (its category changed under it)."""
        return self._set_status(
            proposed_rule_id,
            status="superseded",
            decided=False,
            action="proposed_rule.supersede",
            actor=actor,
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
        )

    def mark_approved(
        self,
        proposed_rule_id: str,
        *,
        rule_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Mark a proposal ``approved`` and link the promoted rule.

        ``parent_audit_id`` threads the rule-INSERT's audit id so the promotion
        cascade (rule create → proposal approve) is one chain (Req 5).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(proposed_rule_id),
                "proposed_rule_id",
                proposed_rule_id,
            )
            self._db.execute(
                f"""
                UPDATE {PROPOSED_RULES.full_name}
                SET status = 'approved', rule_id = ?,
                    decided_at = CURRENT_TIMESTAMP, decided_by = 'user'
                WHERE proposed_rule_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [rule_id, proposed_rule_id],
            )
            after = self._fetch_row(proposed_rule_id)
            return self._emit_audit(
                action="proposed_rule.approve",
                target=(*self._AUDIT_TARGET, proposed_rule_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def mark_rejected(
        self,
        proposed_rule_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Mark a proposal ``rejected`` (no rule created)."""
        return self._set_status(
            proposed_rule_id,
            status="rejected",
            decided=True,
            action="proposed_rule.reject",
            actor=actor,
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
        )

    def _set_status(
        self,
        proposed_rule_id: str,
        *,
        status: str,
        decided: bool,
        action: str,
        actor: str,
        parent_audit_id: str | None,
        in_outer_txn: bool,
    ) -> AuditEvent:
        """Shared status-only transition (supersede/reject); full before/after.

        ``decided`` stamps ``decided_at``/``decided_by`` for terminal user
        decisions (reject); supersede is an automatic transition and leaves
        them untouched. ``status`` is a code-supplied literal, never user input.
        """
        decided_sql = (
            ", decided_at = CURRENT_TIMESTAMP, decided_by = 'user'" if decided else ""
        )
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(proposed_rule_id),
                "proposed_rule_id",
                proposed_rule_id,
            )
            self._db.execute(
                f"UPDATE {PROPOSED_RULES.full_name} "  # noqa: S608  # TableRef + literal status + parameterized id
                f"SET status = ?{decided_sql} WHERE proposed_rule_id = ?",
                [status, proposed_rule_id],
            )
            after = self._fetch_row(proposed_rule_id)
            return self._emit_audit(
                action=action,
                target=(*self._AUDIT_TARGET, proposed_rule_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
