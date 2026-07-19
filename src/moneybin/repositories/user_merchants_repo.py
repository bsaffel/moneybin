"""Audited writes to ``app.user_merchants`` (merchant patterns + exemplar sets).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``CategorizationService``
(via its applier) composes this instead of issuing raw mutation SQL.

Category-id resolution stays in the applier (a read against
``core.dim_categories``); the repo receives the resolved ``category_id`` and
owns only the mutation + audit.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import USER_MERCHANTS

_USER_MERCHANTS_COLUMNS = (
    "merchant_id",
    "raw_pattern",
    "match_type",
    "canonical_name",
    "category",
    "subcategory",
    "category_id",
    "created_by",
    "exemplars",
    "created_at",
    "updated_at",
)


class UserMerchantsRepo(BaseRepo):
    """Audited CRUD over ``app.user_merchants`` (merchant mappings)."""

    repository = "user_merchants"

    table_ref = USER_MERCHANTS
    pk_columns = ("merchant_id",)

    def _fetch_row(self, merchant_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            USER_MERCHANTS, _USER_MERCHANTS_COLUMNS, "merchant_id", merchant_id
        )

    def insert(
        self,
        *,
        raw_pattern: str | None,
        match_type: str,
        canonical_name: str,
        category: str | None,
        subcategory: str | None,
        category_id: str | None,
        created_by: str,
        exemplars: Sequence[str] | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a merchant mapping + audit. ``target_id`` is the new merchant id."""
        merchant_id = uuid.uuid4().hex[:12]
        # DuckDB binds Python lists to VARCHAR[]. An empty list keeps the column
        # default semantics intact for non-exemplar merchants.
        exemplars_param: list[str] = list(exemplars) if exemplars else []
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {USER_MERCHANTS.full_name}
                    (merchant_id, raw_pattern, match_type, canonical_name,
                     category, subcategory, category_id, created_by, exemplars,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    merchant_id,
                    raw_pattern,
                    match_type,
                    canonical_name,
                    category,
                    subcategory,
                    category_id,
                    created_by,
                    exemplars_param,
                ],
            )
            after = self._fetch_row(merchant_id)
            return self._emit_audit(
                action="user_merchant.insert",
                target=(*self._audit_target, merchant_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def append_exemplar(
        self,
        merchant_id: str,
        match_text: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Append ``match_text`` to a merchant's exemplar set; capture before/after.

        ``updated_at`` advances only when the set actually grows (the SET is a
        no-op when the exemplar is already present), per
        ``core-updated-at-convention.md``. The audit row is emitted regardless,
        matching every other repo method's emit-on-call posture.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(merchant_id), "merchant_id", merchant_id
            )
            self._db.execute(
                f"""
                UPDATE {USER_MERCHANTS.full_name}
                SET exemplars = CASE
                        WHEN list_contains(exemplars, ?) THEN exemplars
                        ELSE list_append(exemplars, ?)
                    END,
                    updated_at = CASE
                        WHEN list_contains(exemplars, ?) THEN updated_at
                        ELSE CURRENT_TIMESTAMP
                    END
                WHERE merchant_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [match_text, match_text, match_text, merchant_id],
            )
            after = self._fetch_row(merchant_id)
            return self._emit_audit(
                action="user_merchant.append_exemplar",
                target=(*self._audit_target, merchant_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
