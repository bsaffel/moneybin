"""Audited writes to ``app.rule_conflicts`` (categorization rule-conflict queue).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. Rule creation records
a refused proposal here; the review surface resolves it. Neither issues raw
mutation SQL.

The conflict's identity (``conflict_id``) is computed by the caller — the same
division as ``CategorizationRulesRepo``, which receives a resolved
``category_id`` rather than resolving it. It is a content hash over the
existing rule, that rule's ``updated_at``, the shared matcher digest, and the
proposed category, so re-detecting the same conflict updates one row while an
edit to the existing rule produces a different one.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from moneybin.metrics.registry import RULE_CONFLICTS_PENDING
from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import RULE_CONFLICTS

_RULE_CONFLICTS_COLUMNS = (
    "conflict_id",
    "matcher_digest",
    "existing_rule_id",
    "existing_rule_updated_at",
    "existing_name",
    "existing_category",
    "existing_subcategory",
    "existing_priority",
    "proposed_name",
    "proposed_merchant_pattern",
    "proposed_match_type",
    "proposed_min_amount",
    "proposed_max_amount",
    "proposed_account_id",
    "proposed_category",
    "proposed_subcategory",
    "proposed_priority",
    "proposed_created_by",
    "status",
    "resolution",
    "resolved_rule_id",
    "detected_at",
    "resolved_at",
)

#: The three decisions a user may take on a conflict. ``replace`` supersedes the
#: existing rule with the proposal; ``reprioritize`` activates the proposal
#: alongside it at an explicit priority; ``cancel`` keeps live state as it is.
ConflictResolution = Literal["replace", "reprioritize", "cancel"]


class RuleConflictsRepo(BaseRepo):
    """Audited CRUD over ``app.rule_conflicts``."""

    repository = "rule_conflicts"

    table_ref = RULE_CONFLICTS
    pk_columns = ("conflict_id",)

    def _fetch_row(self, conflict_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            RULE_CONFLICTS, _RULE_CONFLICTS_COLUMNS, "conflict_id", conflict_id
        )

    def fetch(self, conflict_id: str) -> dict[str, Any] | None:
        """Read one conflict row, or ``None``. Reads stay free of audit (Req 2)."""
        return self._fetch_row(conflict_id)

    def set(
        self,
        *,
        conflict_id: str,
        matcher_digest: str,
        existing_rule_id: str,
        existing_rule_updated_at: Any,
        existing_name: str,
        existing_category: str,
        existing_subcategory: str | None,
        existing_priority: int,
        proposed_name: str,
        proposed_merchant_pattern: str,
        proposed_match_type: str,
        proposed_min_amount: Decimal | float | None,
        proposed_max_amount: Decimal | float | None,
        proposed_account_id: str | None,
        proposed_category: str,
        proposed_subcategory: str | None,
        proposed_priority: int,
        proposed_created_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Record a pending conflict + audit; re-detection re-opens the same row.

        An upsert rather than an insert: creating the same refused rule twice is
        one conflict awaiting one decision, not two queue entries. Re-detection
        after a ``cancel`` reopens the row — the user declined that proposal
        once, and submitting it again is a fresh ask.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(conflict_id)
            self._db.execute(
                f"""
                INSERT INTO {RULE_CONFLICTS.full_name}
                    (conflict_id, matcher_digest, existing_rule_id,
                     existing_rule_updated_at, existing_name, existing_category,
                     existing_subcategory, existing_priority,
                     proposed_name, proposed_merchant_pattern, proposed_match_type,
                     proposed_min_amount, proposed_max_amount, proposed_account_id,
                     proposed_category, proposed_subcategory, proposed_priority,
                     proposed_created_by, status, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        'pending', now())
                ON CONFLICT (conflict_id) DO UPDATE SET
                    proposed_name = EXCLUDED.proposed_name,
                    proposed_priority = EXCLUDED.proposed_priority,
                    proposed_created_by = EXCLUDED.proposed_created_by,
                    status = 'pending',
                    resolution = NULL,
                    resolved_rule_id = NULL,
                    resolved_at = NULL,
                    detected_at = now()
                """,  # noqa: S608  # TableRef + parameterized values  # now(), not CURRENT_TIMESTAMP: DuckDB binds the bare keyword inside ON CONFLICT DO UPDATE as a column name and fails
                [
                    conflict_id,
                    matcher_digest,
                    existing_rule_id,
                    existing_rule_updated_at,
                    existing_name,
                    existing_category,
                    existing_subcategory,
                    existing_priority,
                    proposed_name,
                    proposed_merchant_pattern,
                    proposed_match_type,
                    proposed_min_amount,
                    proposed_max_amount,
                    proposed_account_id,
                    proposed_category,
                    proposed_subcategory,
                    proposed_priority,
                    proposed_created_by,
                ],
            )
            after = self._fetch_row(conflict_id)
            return self._emit_audit(
                action="rule_conflict.set",
                target=(*self._audit_target, conflict_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def resolve(
        self,
        conflict_id: str,
        *,
        resolution: ConflictResolution,
        resolved_rule_id: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Settle one conflict with the decision taken + audit the before-image.

        The rule writes the decision implies (superseding the existing rule,
        activating the proposal) are the caller's, composed into the same
        transaction so a half-applied resolution cannot survive a failure.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(conflict_id), "conflict_id", conflict_id
            )
            self._db.execute(
                f"""
                UPDATE {RULE_CONFLICTS.full_name}
                SET status = 'resolved',
                    resolution = ?,
                    resolved_rule_id = ?,
                    resolved_at = CURRENT_TIMESTAMP
                WHERE conflict_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [resolution, resolved_rule_id, conflict_id],
            )
            after = self._fetch_row(conflict_id)
            return self._emit_audit(
                action="rule_conflict.resolve",
                target=(*self._audit_target, conflict_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def prune_stale(
        self,
        existing_rule_id: str,
        *,
        keep_updated_at: Any,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> list[AuditEvent]:
        """Delete *pending* conflicts recorded against an older version of one rule.

        A conflict describes a decision about a rule *as it then was*. Editing
        that rule moves its ``updated_at``, and a still-undecided row stops
        describing anything live — so it is removed rather than left to be
        filtered out on every read.

        A resolved row is not stale, it is settled: it records a decision the
        user actually made, and ``reviews(kind='rule_conflicts',
        status='history')`` publishes it. Pruning without the status predicate
        deleted that history whenever the rule was later edited and re-queued.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            stale_ids = [
                str(row[0])
                for row in self._db.execute(
                    f"SELECT conflict_id FROM {RULE_CONFLICTS.full_name} "  # noqa: S608  # TableRef + parameterized values
                    "WHERE existing_rule_id = ? "
                    "AND status = 'pending' "
                    "AND existing_rule_updated_at IS DISTINCT FROM ? "
                    "ORDER BY conflict_id",
                    [existing_rule_id, keep_updated_at],
                ).fetchall()
            ]
            events: list[AuditEvent] = []
            for conflict_id in stale_ids:
                before = self._fetch_row(conflict_id)
                self._db.execute(
                    f"DELETE FROM {RULE_CONFLICTS.full_name} WHERE conflict_id = ?",  # noqa: S608  # TableRef + parameterized value
                    [conflict_id],
                )
                events.append(
                    self._emit_audit(
                        action="rule_conflict.delete",
                        target=(*self._audit_target, conflict_id),
                        before=self._serialize_for_audit(before),
                        after=None,
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                        context={"reason": "existing_rule_edited"},
                    )
                )
            return events

    def refresh_pending_gauge(self) -> None:
        """Re-read the pending-conflict count after an undo reverses a row."""
        row = self._db.execute(
            f"SELECT COUNT(*) FROM {RULE_CONFLICTS.full_name} "  # noqa: S608  # TableRef constant
            "WHERE status = 'pending'"
        ).fetchone()
        RULE_CONFLICTS_PENDING.set(int(row[0]) if row else 0)
