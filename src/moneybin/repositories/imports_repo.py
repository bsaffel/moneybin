"""Audited writes to ``app.imports`` (user-applied labels on import batches).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``ImportService``
composes this instead of issuing raw mutation SQL for the labels write.

``app.imports`` holds only the labels overlay (one row per labeled import); the
import *lifecycle* writes ``raw.import_log`` (out of scope). ``set`` emits one
full-row ``import.set`` audit (Req 4) — the same idempotent-upsert shape every
other repo uses — rather than per-label semantic events; the full before/after
``labels`` lists make a label change fully reconstructable for Phase 2 undo.
"""

from __future__ import annotations

from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import IMPORTS

_IMPORTS_COLUMNS = (
    "import_id",
    "labels",
    "updated_at",
    "updated_by",
)


class ImportsRepo(BaseRepo):
    """Audited upsert over ``app.imports`` (label overlay)."""

    repository = "imports"

    _AUDIT_TARGET = (IMPORTS.schema, IMPORTS.name)

    def _fetch_row(self, import_id: str) -> dict[str, Any] | None:
        return self._fetch_one(IMPORTS, _IMPORTS_COLUMNS, "import_id", import_id)

    def set(
        self,
        import_id: str,
        *,
        labels: list[str],
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Upsert the labels for one import (INSERT…ON CONFLICT) + audit.

        ``before`` is the prior row when the import already had a label row, else
        ``None``; ``after`` is the resulting row. ``updated_by`` is set to
        ``actor`` (the surface), matching the column's ``'cli'``/``'mcp'``
        semantics. ``target_id`` is ``import_id``.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(import_id)
            self._db.execute(
                f"""
                INSERT INTO {IMPORTS.full_name} (import_id, labels, updated_at, updated_by)
                VALUES (?, ?, NOW(), ?)
                ON CONFLICT (import_id) DO UPDATE SET
                    labels = EXCLUDED.labels,
                    updated_at = NOW(),
                    updated_by = EXCLUDED.updated_by
                """,  # noqa: S608  # TableRef + parameterized values
                [import_id, labels, actor],
            )
            after = self._fetch_row(import_id)
            return self._emit_audit(
                action="import.set",
                target=(*self._AUDIT_TARGET, import_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
