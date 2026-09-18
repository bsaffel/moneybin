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

    def upsert(
        self,
        *,
        source_type: str,
        category: str,
        subcategory: str | None,
        category_id: str,
        code_level: str = "detailed",
        source_taxonomy_version: str | None = None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Upsert one provider/exporter category-code mapping.

        ``source_category_code`` is derived, not accepted directly: an
        imported row carries ``category`` and ``subcategory`` as independent
        text, but the table's key is one string. Encoding both via DuckDB's
        own ``to_json`` (see ``_shared.source_category_code_expr``) keeps the
        code lossless and delimiter-free — the exact same expression the
        read-side bridge lookup in
        ``CategorizationOrchestrator._source_category_bridge_candidates``
        uses, so the two sides can never drift apart.

        ``source_type`` holds a provider's own vocabulary tag for Plaid-style
        rows (``'plaid'``) but a ``source_origin`` value for an imported
        mapping (``'chase_credit'``, ``'mint'``) per the owner's ruling —
        two exporters must be free to map the same category string to two
        different MoneyBin categories.
        """
        # Deferred import: ``_shared`` lives under ``services.categorization``,
        # whose __init__ imports the applier (which imports this repo). A
        # module-level import would form a cycle; by call time the package is
        # initialized. Mirrors ``TransactionCategoriesRepo.upsert_guarded``'s
        # deferred import of ``priority_case_sql`` for the same reason.
        from moneybin.services.categorization._shared import (
            source_category_code_expr,
        )

        code_row = self._db.execute(
            f"SELECT {source_category_code_expr('?', '?')}",  # code-constant SQL expression; values parameterized
            [category, subcategory],
        ).fetchone()
        source_category_code = str(code_row[0]) if code_row is not None else ""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(source_type, source_category_code)
            self._db.execute(
                f"""
                INSERT INTO {CATEGORY_SOURCE_MAP.full_name}
                    (source_type, source_category_code, code_level, category_id,
                     source_taxonomy_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (source_type, source_category_code) DO UPDATE SET
                    code_level = EXCLUDED.code_level,
                    category_id = EXCLUDED.category_id,
                    source_taxonomy_version = EXCLUDED.source_taxonomy_version,
                    updated_at = EXCLUDED.updated_at
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    source_type,
                    source_category_code,
                    code_level,
                    category_id,
                    source_taxonomy_version,
                ],
            )
            after = self._fetch_row(source_type, source_category_code)
            return self._emit_audit(
                action="category_source_map.upsert",
                target=(
                    *self._audit_target,
                    self._target_id(source_type, source_category_code),
                ),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
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
