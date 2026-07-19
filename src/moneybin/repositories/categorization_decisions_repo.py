"""Audited lifecycle for ``app.categorization_decisions``."""

from __future__ import annotations

import hashlib
from typing import Any, Literal

from moneybin.repositories.base import BaseRepo
from moneybin.tables import CATEGORIZATION_DECISIONS

_COLUMNS = (
    "decision_id",
    "transaction_id",
    "status",
    "category_id",
    "merchant_id",
    "proposed_at",
    "decided_at",
    "decided_by",
)


def categorization_decision_id(transaction_id: str) -> str:
    """Return the deterministic proposal ID for one canonical transaction."""
    digest = hashlib.sha256(transaction_id.encode()).hexdigest()[:16]
    return f"cat_{digest}"


class CategorizationDecisionsRepo(BaseRepo):
    """Audited pending → accepted/rejected categorization decisions."""

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
        """Return one exact decision row by ID."""
        return self._fetch_row(decision_id)

    def fetch_by_transaction_id(self, transaction_id: str) -> dict[str, Any] | None:
        """Return one decision row for a canonical transaction."""
        row = self._db.execute(
            f"""
            SELECT decision_id
            FROM {CATEGORIZATION_DECISIONS.full_name}
            WHERE transaction_id = ?
            """,  # noqa: S608  # TableRef + parameterized value
            [transaction_id],
        ).fetchone()
        return self._fetch_row(str(row[0])) if row is not None else None

    def ensure_pending(
        self,
        transaction_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> dict[str, Any]:
        """Materialize one deterministic pending proposal, or return its row."""
        decision_id = categorization_decision_id(transaction_id)
        with self._transaction(in_outer_txn=in_outer_txn):
            existing = self._fetch_row(decision_id)
            if existing is not None:
                return existing
            self._db.execute(
                f"""
                INSERT INTO {CATEGORIZATION_DECISIONS.full_name} (
                    decision_id, transaction_id, status
                ) VALUES (?, ?, 'pending')
                """,  # noqa: S608  # TableRef + parameterized values
                [decision_id, transaction_id],
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
        """Transition one pending proposal to a terminal decision."""
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
            self._db.execute(
                f"""
                UPDATE {CATEGORIZATION_DECISIONS.full_name}
                SET status = ?, category_id = ?, merchant_id = ?,
                    decided_at = CURRENT_TIMESTAMP, decided_by = ?
                WHERE decision_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [status, category_id, merchant_id, decided_by, decision_id],
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

    def list_pending(self) -> list[dict[str, Any]]:
        """Return pending proposals in deterministic proposal order."""
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
        """Return terminal decisions newest-first."""
        rows = self._db.execute(
            f"""
            SELECT decision_id
            FROM {CATEGORIZATION_DECISIONS.full_name}
            WHERE status IN ('accepted', 'rejected')
            ORDER BY decided_at DESC, decision_id DESC
            """,  # noqa: S608  # TableRef constant
        ).fetchall()
        return [
            self._require(self._fetch_row(str(row[0])), "decision_id", row[0])
            for row in rows
        ]
