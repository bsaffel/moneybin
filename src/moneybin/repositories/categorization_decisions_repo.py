"""Audited versioned lifecycle for ``app.categorization_decisions``."""

from __future__ import annotations

import hashlib
from typing import Any, Literal

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import (
    AUDIT_LOG,
    CATEGORIZATION_DECISIONS,
    TRANSACTION_CATEGORIES,
)

_COLUMNS = (
    "decision_id",
    "transaction_id",
    "attempt_number",
    "status",
    "category_id",
    "merchant_id",
    "category",
    "subcategory",
    "categorized_by",
    "confidence",
    "rule_id",
    "source_type",
    "category_revision",
    "proposed_at",
    "decided_at",
    "decided_by",
    "reversed_at",
    "reversed_by",
)


def categorization_decision_id(
    transaction_id: str,
    *,
    attempt_number: int = 1,
) -> str:
    """Return the deterministic ID for one transaction proposal attempt."""
    if attempt_number < 1:
        raise ValueError("attempt_number must be at least 1")
    digest = hashlib.sha256(transaction_id.encode()).hexdigest()[:16]
    base = f"cat_{digest}"
    return base if attempt_number == 1 else f"{base}_a{attempt_number}"


class CategorizationDecisionsRepo(BaseRepo):
    """Audited append-only categorization proposal attempts."""

    repository = "categorization_decisions"
    table_ref = CATEGORIZATION_DECISIONS
    pk_columns = ("decision_id",)

    def _fetch_row(self, decision_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            CATEGORIZATION_DECISIONS,
            _COLUMNS,
            "decision_id",
            decision_id,
        )

    def fetch_by_id(self, decision_id: str) -> dict[str, Any] | None:
        """Return one exact decision attempt by ID."""
        return self._fetch_row(decision_id)

    def fetch_by_transaction_id(self, transaction_id: str) -> dict[str, Any] | None:
        """Return the latest attempt for one canonical transaction."""
        row = self._db.execute(
            f"""
            SELECT decision_id
            FROM {CATEGORIZATION_DECISIONS.full_name}
            WHERE transaction_id = ?
            ORDER BY attempt_number DESC
            LIMIT 1
            """,  # noqa: S608  # TableRef + parameterized value
            [transaction_id],
        ).fetchone()
        return self._fetch_row(str(row[0])) if row is not None else None

    def _category_revision(self, transaction_id: str) -> int:
        row = self._db.execute(
            f"""
            SELECT COUNT(*)
            FROM {AUDIT_LOG.full_name}
            WHERE target_schema = 'app'
              AND target_table = 'transaction_categories'
              AND target_id = ?
            """,  # noqa: S608  # TableRef + parameterized value
            [transaction_id],
        ).fetchone()
        return int(row[0]) if row is not None else 0

    def project_pending_attempts(
        self,
        transaction_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Project current pending attempts for many transactions in one query."""
        ordered_ids = list(dict.fromkeys(transaction_ids))
        if not ordered_ids:
            return {}
        values_sql = ", ".join("(?)" for _ in ordered_ids)
        decision_columns = ", ".join(f"d.{column}" for column in _COLUMNS)
        rows = self._db.execute(
            f"""
            WITH requested(transaction_id) AS (
                VALUES {values_sql}
            )
            SELECT r.transaction_id, {decision_columns},
                   (
                       SELECT COUNT(*)
                       FROM {AUDIT_LOG.full_name} AS audit
                       WHERE audit.target_schema = 'app'
                         AND audit.target_table = 'transaction_categories'
                         AND audit.target_id = r.transaction_id
                   ) AS current_revision,
                   EXISTS (
                       SELECT 1
                       FROM {TRANSACTION_CATEGORIES.full_name} AS tc
                       WHERE tc.transaction_id = r.transaction_id
                   ) AS is_categorized
            FROM requested AS r
            LEFT JOIN LATERAL (
                SELECT *
                FROM {CATEGORIZATION_DECISIONS.full_name} AS attempts
                WHERE attempts.transaction_id = r.transaction_id
                ORDER BY attempts.attempt_number DESC
                LIMIT 1
            ) AS d ON TRUE
            """,  # noqa: S608  # Placeholder count only; TableRef constants
            ordered_ids,
        ).fetchall()
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            transaction_id = str(row[0])
            latest_values = row[1 : 1 + len(_COLUMNS)]
            current_revision = int(row[-2])
            is_categorized = bool(row[-1])
            if is_categorized:
                continue
            latest: dict[str, Any] | None = None
            if latest_values[0] is not None:
                latest = dict(zip(_COLUMNS, latest_values, strict=True))
            if (
                latest is not None
                and latest["status"] == "rejected"
                and latest["reversed_at"] is None
                and int(latest["category_revision"]) == current_revision
            ):
                continue
            if (
                latest is not None
                and latest["status"] == "pending"
                and int(latest["category_revision"]) == current_revision
            ):
                result[transaction_id] = latest
                continue
            attempt_number = (
                int(latest["attempt_number"]) + 1 if latest is not None else 1
            )
            pending: dict[str, Any] = dict.fromkeys(_COLUMNS)
            pending.update({
                "decision_id": categorization_decision_id(
                    transaction_id,
                    attempt_number=attempt_number,
                ),
                "transaction_id": transaction_id,
                "attempt_number": attempt_number,
                "status": "pending",
                "category_revision": current_revision,
            })
            result[transaction_id] = pending
        return result

    def ensure_pending(
        self,
        transaction_id: str,
        *,
        actor: str,
        expected_decision_id: str | None = None,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> dict[str, Any]:
        """Materialize the projected pending attempt without reopening history."""
        with self._transaction(in_outer_txn=in_outer_txn):
            projected = self.project_pending_attempts([transaction_id]).get(
                transaction_id
            )
            if projected is None:
                raise ValueError(
                    f"transaction_id={transaction_id!r} has no pending "
                    "categorization attempt"
                )
            decision_id = str(projected["decision_id"])
            if expected_decision_id is not None and expected_decision_id != decision_id:
                raise ValueError(
                    f"expected decision_id={expected_decision_id!r}, "
                    f"but current pending attempt is {decision_id!r}"
                )
            existing = self._fetch_row(decision_id)
            if existing is not None:
                return existing
            latest = self.fetch_by_transaction_id(transaction_id)
            if latest is not None and latest["status"] == "pending":
                self._supersede(
                    latest,
                    actor=actor,
                    parent_audit_id=parent_audit_id,
                )
            self._db.execute(
                f"""
                INSERT INTO {CATEGORIZATION_DECISIONS.full_name} (
                    decision_id, transaction_id, attempt_number, status,
                    category_revision
                ) VALUES (?, ?, ?, 'pending', ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    decision_id,
                    transaction_id,
                    projected["attempt_number"],
                    projected["category_revision"],
                ],
            )
            after = self._fetch_row(decision_id)
            self._emit_audit(
                action="categorization_decision.insert",
                target=(*self._audit_target, decision_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
            return self._require(after, "decision_id", decision_id)

    def _supersede(
        self,
        before: dict[str, Any],
        *,
        actor: str,
        parent_audit_id: str | None,
    ) -> None:
        decision_id = str(before["decision_id"])
        self._db.execute(
            f"""
            UPDATE {CATEGORIZATION_DECISIONS.full_name}
            SET status = 'superseded', decided_at = CURRENT_TIMESTAMP,
                decided_by = 'system'
            WHERE decision_id = ? AND status = 'pending'
            """,  # noqa: S608  # TableRef + parameterized value
            [decision_id],
        )
        after = self._fetch_row(decision_id)
        self._emit_audit(
            action="categorization_decision.supersede",
            target=(*self._audit_target, decision_id),
            before=self._serialize_for_audit(before),
            after=self._serialize_for_audit(after),
            actor=actor,
            parent_audit_id=parent_audit_id,
        )

    def update_status(
        self,
        decision_id: str,
        *,
        status: Literal["accepted", "rejected"],
        category_id: str | None,
        merchant_id: str | None,
        decided_by: Literal["user", "system"],
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> dict[str, Any]:
        """Transition one pending attempt and freeze its accepted target state."""
        if status == "accepted" and category_id is None:
            raise ValueError("accepted categorization decisions require category_id")
        if status == "rejected" and (
            category_id is not None or merchant_id is not None
        ):
            raise ValueError("rejected categorization decisions forbid targets")
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(decision_id),
                "decision_id",
                decision_id,
            )
            if before["status"] != "pending":
                raise ValueError(
                    f"decision_id={decision_id!r} is already terminal "
                    f"with status {before['status']!r}"
                )
            snapshots: dict[str, Any] = {
                "category": None,
                "subcategory": None,
                "categorized_by": None,
                "confidence": None,
                "rule_id": None,
                "source_type": None,
            }
            if status == "accepted":
                row = self._db.execute(
                    f"""
                    SELECT category, subcategory, category_id, categorized_by,
                           confidence, rule_id, source_type
                    FROM {TRANSACTION_CATEGORIES.full_name}
                    WHERE transaction_id = ?
                    """,  # noqa: S608  # TableRef + parameterized value
                    [before["transaction_id"]],
                ).fetchone()
                if row is None or row[2] is None:
                    raise ValueError(
                        "accepted categorization decision requires a current "
                        "canonical transaction category"
                    )
                if str(row[2]) != category_id:
                    raise ValueError(
                        "accepted categorization decision category_id does not "
                        "match the current transaction category"
                    )
                snapshots = {
                    "category": row[0],
                    "subcategory": row[1],
                    "categorized_by": row[3],
                    "confidence": row[4],
                    "rule_id": row[5],
                    "source_type": row[6],
                }
            revision = self._category_revision(str(before["transaction_id"]))
            self._db.execute(
                f"""
                UPDATE {CATEGORIZATION_DECISIONS.full_name}
                SET status = ?, category_id = ?, merchant_id = ?,
                    category = ?, subcategory = ?, categorized_by = ?,
                    confidence = ?, rule_id = ?, source_type = ?,
                    category_revision = ?, decided_at = CURRENT_TIMESTAMP,
                    decided_by = ?
                WHERE decision_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    status,
                    category_id,
                    merchant_id,
                    snapshots["category"],
                    snapshots["subcategory"],
                    snapshots["categorized_by"],
                    snapshots["confidence"],
                    snapshots["rule_id"],
                    snapshots["source_type"],
                    revision,
                    decided_by,
                    decision_id,
                ],
            )
            after = self._fetch_row(decision_id)
            self._emit_audit(
                action="categorization_decision.update_status",
                target=(*self._audit_target, decision_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
            return self._require(after, "decision_id", decision_id)

    def undo_event(
        self,
        event: AuditEvent,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Reverse outcome liveness without deleting or reopening an attempt."""
        if event.is_undo:
            return super().undo_event(
                event,
                actor=actor,
                in_outer_txn=in_outer_txn,
            )
        with self._transaction(in_outer_txn=in_outer_txn):
            current = self._require(
                self._fetch_row(str(event.target_id)),
                "decision_id",
                event.target_id,
            )
            before = self._serialize_for_audit(current)
            if (
                event.action == "categorization_decision.update_status"
                and current["status"] in ("accepted", "rejected")
                and current["reversed_at"] is None
            ):
                self._db.execute(
                    f"""
                    UPDATE {CATEGORIZATION_DECISIONS.full_name}
                    SET reversed_at = CURRENT_TIMESTAMP, reversed_by = ?
                    WHERE decision_id = ?
                    """,  # noqa: S608  # TableRef + parameterized values
                    [actor, event.target_id],
                )
            after = self._serialize_for_audit(self._fetch_row(str(event.target_id)))
            return self._emit_audit(
                action=f"{event.action}.undo",
                target=(*self._audit_target, str(event.target_id)),
                before=before,
                after=after,
                actor=actor,
                is_undo=True,
                undoes_operation_id=event.operation_id,
            )

    def list_pending(self) -> list[dict[str, Any]]:
        """Return materialized pending attempts in proposal order."""
        rows = self._db.execute(
            f"""
            SELECT decision_id
            FROM {CATEGORIZATION_DECISIONS.full_name}
            WHERE status = 'pending'
            ORDER BY proposed_at, decision_id
            """,  # noqa: S608  # TableRef constant
        ).fetchall()
        return [
            self._require(self._fetch_row(str(row[0])), "decision_id", row[0])
            for row in rows
        ]

    def history(self) -> list[dict[str, Any]]:
        """Return preserved terminal and superseded attempts newest-first."""
        rows = self._db.execute(
            f"""
            SELECT decision_id
            FROM {CATEGORIZATION_DECISIONS.full_name}
            WHERE status IN ('accepted', 'rejected', 'superseded')
            ORDER BY COALESCE(decided_at, proposed_at) DESC,
                     attempt_number DESC,
                     decision_id DESC
            """,  # noqa: S608  # TableRef constant
        ).fetchall()
        return [
            self._require(self._fetch_row(str(row[0])), "decision_id", row[0])
            for row in rows
        ]
