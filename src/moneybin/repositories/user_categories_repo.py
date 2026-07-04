"""Audited writes to ``app.user_categories`` and ``app.category_overrides``.

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation
of these tables flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``CategorizationService``
(via its applier) composes these instead of issuing raw mutation SQL.

The two tables are co-located because the service mutates them together:
``toggle_category`` branches on whether a category is a seeded default
(→ ``category_overrides``) or user-created (→ ``user_categories``).
"""

from __future__ import annotations

import uuid
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import CATEGORY_OVERRIDES, USER_CATEGORIES

_USER_CATEGORIES_COLUMNS = (
    "category_id",
    "category",
    "subcategory",
    "description",
    "class",
    "is_active",
    "created_at",
    "updated_at",
)

_CATEGORY_OVERRIDES_COLUMNS = (
    "category_id",
    "is_active",
    "updated_at",
)


class UserCategoriesRepo(BaseRepo):
    """Audited CRUD over ``app.user_categories`` (user-created categories)."""

    repository = "user_categories"

    table_ref = USER_CATEGORIES
    pk_columns = ("category_id",)

    def _fetch_row(self, category_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            USER_CATEGORIES, _USER_CATEGORIES_COLUMNS, "category_id", category_id
        )

    def insert(
        self,
        *,
        category: str,
        subcategory: str | None = None,
        description: str | None = None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a new user category (active) + audit. ``target_id`` is the new id."""
        category_id = uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {USER_CATEGORIES.full_name}
                    (category_id, category, subcategory, description,
                     is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [category_id, category, subcategory, description],
            )
            after = self._fetch_row(category_id)
            return self._emit_audit(
                action="user_category.insert",
                target=(*self._audit_target, category_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def update_active(
        self,
        category_id: str,
        *,
        is_active: bool,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Set ``is_active`` on a user category; capture full before/after."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(category_id), "category_id", category_id
            )
            self._db.execute(
                f"UPDATE {USER_CATEGORIES.full_name} "  # noqa: S608  # TableRef constant
                f"SET is_active = ?, updated_at = CURRENT_TIMESTAMP "
                f"WHERE category_id = ?",
                [is_active, category_id],
            )
            after = self._fetch_row(category_id)
            return self._emit_audit(
                action="user_category.update_active",
                target=(*self._audit_target, category_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        category_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Hard-delete a user category; capture the full prior row in ``before``."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(category_id), "category_id", category_id
            )
            self._db.execute(
                f"DELETE FROM {USER_CATEGORIES.full_name} WHERE category_id = ?",  # noqa: S608  # TableRef + parameterized value
                [category_id],
            )
            return self._emit_audit(
                action="user_category.delete",
                target=(*self._audit_target, category_id),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )


class CategoryOverridesRepo(BaseRepo):
    """Audited upsert over ``app.category_overrides`` (deactivation of defaults).

    The only mutation users can make to a seeded default category is toggling
    its visibility, recorded here as an INSERT…ON CONFLICT upsert keyed by
    ``category_id``.
    """

    repository = "category_overrides"

    table_ref = CATEGORY_OVERRIDES
    pk_columns = ("category_id",)

    def _fetch_row(self, category_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            CATEGORY_OVERRIDES, _CATEGORY_OVERRIDES_COLUMNS, "category_id", category_id
        )

    def set_active(
        self,
        category_id: str,
        *,
        is_active: bool,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Upsert the active flag for a default category; capture full before/after."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(category_id)
            self._db.execute(
                f"""
                INSERT INTO {CATEGORY_OVERRIDES.full_name}
                    (category_id, is_active, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (category_id) DO UPDATE
                    SET is_active = excluded.is_active,
                        updated_at = excluded.updated_at
                """,  # noqa: S608  # TableRef + parameterized values
                [category_id, is_active],
            )
            after = self._fetch_row(category_id)
            return self._emit_audit(
                action="category_override.set_active",
                target=(*self._audit_target, category_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
