"""Audited writes to user-owned provider category mappings."""

from __future__ import annotations

from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import CATEGORY_SOURCE_MAP

_COLUMNS = (
    "source_type",
    "source_category_code",
    "code_level",
    "category_id",
    "source_taxonomy_version",
    "created_at",
    "updated_at",
)


class CategorySourceMapRepo(BaseRepo):
    """Audited CRUD over ``app.category_source_map``."""

    repository = "category_source_map"
    table_ref = CATEGORY_SOURCE_MAP
    pk_columns = ("source_type", "source_category_code")

    def _fetch_row(
        self,
        source_type: str,
        source_category_code: str,
    ) -> dict[str, Any] | None:
        row = self._db.execute(
            f"""
            SELECT source_type, source_category_code, code_level, category_id,
                   source_taxonomy_version, created_at, updated_at
            FROM {CATEGORY_SOURCE_MAP.full_name}
            WHERE source_type = ? AND source_category_code = ?
            """,  # noqa: S608  # TableRef + parameterized values
            [source_type, source_category_code],
        ).fetchone()
        return dict(zip(_COLUMNS, row, strict=True)) if row is not None else None

    @staticmethod
    def _target_id(source_type: str, source_category_code: str) -> str:
        """Flatten the composite primary key for audit targeting."""
        return f"{source_type}:{source_category_code}"

    def _row_target_id(self, row: dict[str, Any]) -> str:
        """Mirror the forward mutation's composite audit target."""
        return self._target_id(
            str(row["source_type"]),
            str(row["source_category_code"]),
        )

    def delete(
        self,
        *,
        source_type: str,
        source_category_code: str,
        actor: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Delete one provider mapping with its full audit before-image."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(source_type, source_category_code),
                "source mapping",
                self._target_id(source_type, source_category_code),
            )
            self._db.execute(
                f"DELETE FROM {CATEGORY_SOURCE_MAP.full_name} "  # noqa: S608  # TableRef + parameterized values
                "WHERE source_type = ? AND source_category_code = ?",
                [source_type, source_category_code],
            )
            return self._emit_audit(
                action="category_source_map.delete",
                target=(
                    *self._audit_target,
                    self._target_id(source_type, source_category_code),
                ),
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
        """Delete every provider mapping using one category, with per-row audit."""
        with self._transaction(in_outer_txn=in_outer_txn):
            keys = [
                (str(row[0]), str(row[1]))
                for row in self._db.execute(
                    f"""
                    SELECT source_type, source_category_code
                    FROM {CATEGORY_SOURCE_MAP.full_name}
                    WHERE category_id = ?
                    ORDER BY source_type, source_category_code
                    """,  # noqa: S608  # TableRef + parameterized value
                    [category_id],
                ).fetchall()
            ]
            return [
                self.delete(
                    source_type=source_type,
                    source_category_code=source_category_code,
                    actor=actor,
                    in_outer_txn=True,
                )
                for source_type, source_category_code in keys
            ]
